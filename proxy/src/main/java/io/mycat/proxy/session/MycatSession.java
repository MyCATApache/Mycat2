/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.session;

import io.mycat.*;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.mycat.beans.mysql.packet.ProxyBuffer;
import io.mycat.buffer.BufferPool;
import io.mycat.command.CommandDispatcher;
import io.mycat.command.LocalInFileRequestParseHelper.LocalInFileSession;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.buffer.CrossSwapThreadBufferPool;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.handler.MySQLPacketExchanger;
import io.mycat.proxy.handler.MycatSessionWriteHandler;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.FrontMySQLPacketResolver;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Function;

//tcp.port in {8066} or tcp.port in  {3066}
public final class MycatSession extends AbstractSession<MycatSession> implements LocalInFileSession,
        MySQLProxyServerSession<MycatSession> {
    private final static Logger LOGGER = LoggerFactory.getLogger(MycatSession.class);
    private CommandDispatcher commandHandler;

    /**
     * mysql服务器状态
     */
    private final MycatDataContext dataContext;
    /***
     * 以下资源要做session关闭时候释放
     */
    private final ProxyBuffer proxyBuffer;//clearQueue
    private final ByteBuffer header = ByteBuffer.allocate(4);//gc
    private final LinkedTransferQueue<ByteBuffer> writeQueue = new LinkedTransferQueue<>();//buffer recycle
    //  private final MySQLPacketResolver packetResolver = new BackendMySQLPacketResolver(this);//clearQueue
    private final CrossSwapThreadBufferPool crossSwapThreadBufferPool;


    /**
     * 报文写入辅助类
     */
    private final ByteBuffer[] packetContainer = new ByteBuffer[2];
    private final MySQLPacketSplitter packetSplitter = new PacketSplitterImpl();
    private volatile ProcessState processState;//每次在处理请求时候就需要重置
    private MySQLClientSession backend;//unbindSource
    private MycatSessionWriteHandler writeHandler = WriteHandler.INSTANCE;
    private final FrontMySQLPacketResolver frontResolver;
    private byte packetId = 0;
    private final ArrayDeque<NIOJob> delayedNioJobs = new ArrayDeque<>();

    private boolean gracefulShutdowning = false;

    public MycatSession(int sessionId, BufferPool bufferPool, NIOHandler nioHandler,
                        SessionManager<MycatSession> sessionManager,
                        Map<TransactionType, Function<MycatDataContext, TransactionSession>> transcationFactoryMap,
                                MycatContextThreadPool mycatContextThreadPool) {
        super(sessionId, nioHandler, sessionManager);
        this.proxyBuffer = new ProxyBufferImpl(bufferPool);
        this.crossSwapThreadBufferPool = new CrossSwapThreadBufferPool(
                bufferPool);

        this.processState = ProcessState.READY;
        this.frontResolver = new FrontMySQLPacketResolver(bufferPool, this);
        this.packetId = 0;
        this.dataContext = new MycatDataContextImpl(new ServerTransactionSessionRunner(transcationFactoryMap,mycatContextThreadPool,this));
    }

    public void setCommandHandler(CommandDispatcher commandHandler) {
        this.commandHandler = commandHandler;
    }

    public CommandDispatcher getCommandHandler() {
        return commandHandler;
    }

    public void switchWriteHandler(MycatSessionWriteHandler writeHandler) {
        this.writeHandler = writeHandler;
    }

    public void onHandlerFinishedClear() {
        resetPacket();
        setResponseFinished(ProcessState.READY);
        if (!isInTransaction() || !isBindMySQLSession()) {
            //todo
//            if (getRuntime().isGracefulShutdown() && gracefulShutdowning == false) {
//                gracefulShutdowning = true;
//                this.close(true, "gracefulShutdown");
//                return;
//            }
        }
        switch (this.writeHandler.getType()) {
            case SERVER:
                this.change2ReadOpts();
                break;
            case PROXY:
                this.change2ReadOpts();
                break;
        }
    }

    public MySQLIsolation getIsolation() {
        return this.dataContext.getIsolation();
    }

    public void setIsolation(MySQLIsolation isolation) {
        LOGGER.info("set mycat session id:{} isolation:{}", sessionId(), isolation);
        this.dataContext.setIsolation(isolation);
    }


    public void setMultiStatementSupport(boolean on) {

    }

    public void setCharset(String charsetName) {
        setCharset(CharsetUtil.getIndex(charsetName), charsetName);
    }

    public boolean isBindMySQLSession() {
        return backend != null;
    }


    private void setCharset(int index, String charsetName) {
        this.dataContext.setCharset(index, charsetName, Charset.defaultCharset());
    }

    public void setSchema(String schema) {
        this.dataContext.useShcema(schema);
    }

    public MySQLClientSession getMySQLSession() {
        return backend;
    }


    @Override
    public  synchronized  void close(boolean normal, String hint) {
        try {
            dataContext.close();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        if (!normal) {
            assert hint != null;
            setLastMessage(hint);
        }
        assert hint != null;
        try {
            if (crossSwapThreadBufferPool != null) {
                SessionThread source = crossSwapThreadBufferPool.getSource();
                if (source != null) {
                    source.setCurSession(null);
                }
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        try {
            MySQLClientSession sqlSession = getMySQLSession();
            if (sqlSession != null) {
                sqlSession.close(false, hint);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        resetPacket();
        if (this.getMySQLSession() != null) {
            this.getMySQLSession().close(normal, hint);
        }
        hasClosed = true;
        try {
            getSessionManager().removeSession(this, normal, hint);
        } catch (Exception e) {
            LOGGER.error("{}", e);
        }
    }


    public ProxyBuffer currentProxyBuffer() {
        return proxyBuffer;
    }

    public int getServerCapabilities() {
        return this.dataContext.getServerCapabilities();
    }

    public void setServerCapabilities(int serverCapabilities) {
        this.dataContext.setServerCapabilities(serverCapabilities);
    }

    public void setMySQLSession(MySQLClientSession mySQLSession) {
        this.backend = mySQLSession;
    }

    public long getAffectedRows() {
        return this.dataContext.getAffectedRows();
    }

    public void setAffectedRows(long affectedRows) {
        this.dataContext.setAffectedRows(affectedRows);
    }


    @Override
    public Queue<ByteBuffer> writeQueue() {
        return writeQueue;
    }

    ByteBuffer lastWritePacket;

    @Override
    public ByteBuffer lastWritePacket() {
        return lastWritePacket;
    }

    @Override
    public void setLastWritePacket(ByteBuffer buffer) {
        this.lastWritePacket = buffer;
    }

    @Override
    public CrossSwapThreadBufferPool writeBufferPool() {
        return this.crossSwapThreadBufferPool;
    }

    @Override
    public ByteBuffer packetHeaderBuffer() {
        return header;
    }


    @Override
    public ByteBuffer[] packetContainer() {
        return packetContainer;
    }

    @Override
    public void setPacketId(int packetId) {
        this.packetId = (byte) packetId;
    }

    @Override
    public byte getNextPacketId() {
        return ++packetId;
    }


    @Override
    public MySQLPacketSplitter packetSplitter() {
        return packetSplitter;
    }

    @Override
    public void switchProxyWriteHandler() {
        clearReadWriteOpts();
        this.writeHandler = MySQLPacketExchanger.WriteHandler.INSTANCE;
    }

    @Override
    public String getLastMessage() {
        String lastMessage = this.dataContext.getLastMessage();
        return " " + lastMessage + "";
    }

    public String setLastMessage(String lastMessage) {
        this.dataContext.setLastMessage(lastMessage);
        return lastMessage;
    }

    @Override
    public long affectedRows() {
        return this.dataContext.getAffectedRows();
    }


    @Override
    public int getServerStatusValue() {
        return this.dataContext.serverStatus();
    }

    public void setInTranscation(boolean on) {
        this.dataContext.setInTransaction(on);
    }

    public void setLastInsertId(long s) {
        this.dataContext.setLastInsertId(s);
    }

    @Override
    public int getLastErrorCode() {
        return this.dataContext.getLastErrorCode();
    }

    @Override
    public boolean isDeprecateEOF() {
        return MySQLServerCapabilityFlags.isDeprecateEOF(this.dataContext.getServerCapabilities());
    }

    public int getWarningCount() {
        return this.dataContext.getWarningCount();
    }

    public long getLastInsertId() {
        return this.dataContext.getLastInsertId();
    }

    public void resetSession() {
        throw new MycatException("unsupport!");
    }

    @Override
    public Charset charset() {
        return this.dataContext.getCharset();
    }


    @Override
    public int charsetIndex() {
        return this.dataContext.getCharsetIndex();
    }

    @Override
    public int getCapabilities() {
        return this.dataContext.getServerCapabilities();
    }

    @Override
    public void setLastErrorCode(int errorCode) {
        this.dataContext.setVariable(MycatDataContextEnum.LAST_ERROR_CODE, errorCode);
    }

    @Override
    public boolean isResponseFinished() {
        return processState == ProcessState.DONE;
    }

    @Override
    public void setResponseFinished(ProcessState b) {
        this.processState = b;
    }

    @Override
    public void switchMySQLServerWriteHandler() {
        this.clearReadWriteOpts();
        this.writeHandler = WriteHandler.INSTANCE;
    }


    @Override
    public void writeToChannel() throws IOException {
        try {
            writeHandler.writeToChannel(this);
        } catch (Exception e) {
            writeHandler.onException(this, e);
            resetPacket();
            throw e;
        }
    }

    @Override
    public final boolean readFromChannel() throws IOException {
        boolean b = frontResolver.readFromChannel();
        if (b) {
            MycatMonitor.onFrontRead(this, proxyBuffer.currentByteBuffer(),
                    proxyBuffer.channelReadStartIndex(), proxyBuffer.channelReadEndIndex());
        }
        return b;
    }

    public MySQLPacket currentProxyPayload() {
        return frontResolver.getPayload();
    }

    public void resetCurrentProxyPayload() {
        frontResolver.reset();
    }

    public void resetPacket() {
        resetCurrentProxyPayload();
        writeHandler.onClear(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MycatSession that = (MycatSession) o;

        return this.sessionId == that.sessionId;
    }

    @Override
    public int hashCode() {
        return this.sessionId;
    }

    @Override
    public boolean shouldHandleContentOfFilename() {
        return this.dataContext.getVariable(MycatDataContextEnum.IS_LOCAL_IN_FILE_REQUEST_STATE);
    }

    @Override
    public void setHandleContentOfFilename(boolean need) {
        this.dataContext.setVariable(MycatDataContextEnum.IS_LOCAL_IN_FILE_REQUEST_STATE, need);
    }


    @Override
    public void switchNioHandler(NIOHandler nioHandler) {
        this.nioHandler = nioHandler;
    }

    public String getSchema() {
        return this.dataContext.getDefaultSchema();
    }

    public void useSchema(String schema) {
        this.dataContext.useShcema(schema);
    }

    public MycatUser getUser() {
        return dataContext.getUser();
    }

    public void setUser(MycatUser user) {
        this.dataContext.setUser(user);
    }

    public void setCharset(int index) {
        this.setCharset(CharsetUtil.getCharset(index));
    }

    public MycatSessionWriteHandler getWriteHandler() {
        return writeHandler;
    }

    /**
     * 在业务线程使用,在业务线程运行的时候设置业务线程当前的session,方便监听类获取session记录
     */
    public void deliverWorkerThread(SessionThread thread) {
        LOGGER.info("{}", thread);
        crossSwapThreadBufferPool.bindSource(thread);
        assert thread == Thread.currentThread();
    }

    /**
     * 业务线程执行结束,清除业务线程的session,并代表处理结束
     */
    @Override
    public void backFromWorkerThread() {
        Thread thread = Thread.currentThread();
        assert getIOThread() != thread && thread instanceof SessionThread;
        writeBufferPool().bindSource(null);
    }


    public FrontMySQLPacketResolver getMySQLPacketResolver() {
        return frontResolver;
    }

    public ProcessState getProcessState() {
        return processState;
    }


    public void setAutoCommit(boolean autocommit) {
        this.dataContext.setAutoCommit(autocommit);
    }

    public boolean isInTransaction() {
        return dataContext.isInTransaction();
    }

    public boolean isAutocommit() {
        return dataContext.isAutocommit();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        if (iface == MycatDataContext.class) {
            return (T) dataContext;
        }
        return null;
    }

    public MycatDataContext getDataContext() {
        return dataContext;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return unwrap(iface) != null;
    }

    public boolean isIOThreadMode() {
        return Thread.currentThread() == this.getIOThread();
    }

    public void addDelayedNioJob(NIOJob runnable) {
        Objects.requireNonNull(runnable);
        delayedNioJobs.addLast(runnable);
    }

    public void runDelayedNioJob() {
        MycatReactorThread ioThread = getIOThread();
        while (!delayedNioJobs.isEmpty()) {
            ioThread.addNIOJob(delayedNioJobs.pollFirst());
        }
    }

    @Override
    public boolean checkOpen() {
        boolean b = super.checkOpen() && dataContext.isRunning();
        boolean b1 = processState == ProcessState.READY || ((processState == ProcessState.DONE || processState == ProcessState.DOING) && !isIOTimeout());
        return b && b1;
    }
}

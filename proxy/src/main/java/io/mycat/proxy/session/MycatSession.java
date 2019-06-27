/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.session;

import io.mycat.MycatExpection;
import io.mycat.beans.MySQLServerStatus;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.mycat.buffer.BufferPool;
import io.mycat.buffer.CrossSwapThreadBufferPool;
import io.mycat.command.CommandDispatcher;
import io.mycat.command.CommandResolver;
import io.mycat.command.LocalInFileRequestParseHelper.LocalInFileSession;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.logTip.SessionTip;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.handler.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPacketResolver.ComQueryState;
import io.mycat.proxy.packet.MySQLPacketResolverImpl;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.security.MycatUser;
import io.mycat.util.CharsetUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;

public final class MycatSession extends AbstractSession<MycatSession> implements
    MySQLProxySession<MycatSession>, LocalInFileSession,
    MySQLProxyServerSession<MycatSession> {

  private CommandDispatcher commandHandler;
  int resultSetCount;

  /**
   * mysql服务器状态
   */
  private final MySQLServerStatus serverStatus = new MySQLServerStatus();
  /***
   * 以下资源要做session关闭时候释放
   */
  private final ProxyBuffer proxyBuffer;//reset
  private final ByteBuffer header = ByteBuffer.allocate(4);//gc
  private String schema;
  private MycatUser user;
  private final LinkedTransferQueue<ByteBuffer> writeQueue = new LinkedTransferQueue<>();//buffer recycle
  private final MySQLPacketResolver packetResolver = new MySQLPacketResolverImpl(this);//reset
  private final CrossSwapThreadBufferPool crossSwapThreadBufferPool;


  /**
   * 报文写入辅助类
   */
  private final ByteBuffer[] packetContainer = new ByteBuffer[2];
  private final MySQLPacketSplitter packetSplitter = new PacketSplitterImpl();
  private volatile boolean responseFinished = false;//每次在处理请求时候就需要重置
  private MySQLClientSession backend;//unbindSource
  private MycatSessionWriteHandler writeHandler = WriteHandler.INSTANCE;

  public MycatSession(int sessionId, BufferPool bufferPool, NIOHandler nioHandler,
      SessionManager<MycatSession> sessionManager) {
    super(sessionId, nioHandler, sessionManager);
    this.proxyBuffer = new ProxyBufferImpl(bufferPool);
    this.crossSwapThreadBufferPool = new CrossSwapThreadBufferPool(Thread.currentThread(),
        bufferPool);
  }

  /**
   * Setter for property 'commandHandler'.
   *
   * @param commandHandler Value to set for property 'commandHandler'.
   */
  public void setCommandHandler(CommandDispatcher commandHandler) {
    this.commandHandler = commandHandler;
  }

  /**
   * 路由信息
   */
  private String dataNode;

  public void handle() {
    assert commandHandler != null;
    CommandResolver.handle(this, commandHandler);
  }


  public void switchWriteHandler(MycatSessionWriteHandler writeHandler) {
    this.writeHandler = writeHandler;
  }

  public void onHandlerFinishedClear() {
    resetPacket();
    setResponseFinished(false);
  }

  public MySQLAutoCommit getAutoCommit() {
    return this.serverStatus.getAutoCommit();
  }

  public void setAutoCommit(MySQLAutoCommit off) {
    this.serverStatus.setAutoCommit(off);
  }

  public MySQLIsolation getIsolation() {
    return this.serverStatus.getIsolation();
  }

  public void setIsolation(MySQLIsolation readUncommitted) {
    this.serverStatus.setIsolation(readUncommitted);
  }


  public void setMultiStatementSupport(boolean on) {

  }

  public void setCharset(String charsetName) {
    setCharset(CharsetUtil.getIndex(charsetName), charsetName);
  }


  public void setCharsetSetResult(String charsetSetResult) {
    this.serverStatus.setCharsetSetResult(charsetSetResult);
  }


  public boolean isBindMySQLSession() {
    return backend != null;
  }


  private void setCharset(int index, String charsetName) {
    this.serverStatus.setCharset(index, charsetName, Charset.defaultCharset());
  }


  public String getDataNode() {
    return dataNode;
  }

  public void switchDataNode(String dataNode) {
    if (this.backend == null) {
      this.dataNode = dataNode;
      return;
    } else {
      if (dataNode.equals(this.dataNode)) {
        return;
      } else {
        throw new MycatExpection(SessionTip.CANNOT_SWITCH_DATANODE.getMessage());
      }
    }
  }


  public void setSchema(String schema) {
    this.schema = schema;
  }

  public MySQLClientSession getMySQLSession() {
    return backend;
  }


  @Override
  public void close(boolean normal, String hint) {
    if (!normal) {
      assert hint != null;
      setLastMessage(hint);
    }
    assert hint != null;
    try {
      MySQLClientSession sqlSession = getMySQLSession();
      if (sqlSession != null) {
        sqlSession.close(false, hint);
      }
    } catch (Exception e) {
      LOGGER.error("{}", e);
    }
    onHandlerFinishedClear();
    if (this.getMySQLSession() != null) {
      this.getMySQLSession().close(normal, hint);
    }
    closed = true;
    try {
      getSessionManager().removeSession(this, normal, hint);
    } catch (Exception e) {
      LOGGER.error("{}", e);
    }
  }


  @Override
  public ProxyBuffer currentProxyBuffer() {
    return proxyBuffer;
  }

  @Override
  public MySQLPacketResolver getPacketResolver() {
    return packetResolver;
  }

  @Override
  public void setCurrentProxyBuffer(ProxyBuffer buffer) {
    throw new MycatExpection("unsupport!");
  }

  public int getServerCapabilities() {
    return this.serverStatus.getServerCapabilities();
  }

  public void setServerCapabilities(int serverCapabilities) {
    this.serverStatus.setServerCapabilities(serverCapabilities);
    packetResolver.setCapabilityFlags(serverCapabilities);
  }

  public void setMySQLSession(MySQLClientSession mySQLSession) {
    this.backend = mySQLSession;
  }

  public long getAffectedRows() {
    return this.serverStatus.getAffectedRows();
  }

  public void setAffectedRows(long affectedRows) {
    this.serverStatus.setAffectedRows(affectedRows);
  }


  @Override
  public Queue<ByteBuffer> writeQueue() {
    return writeQueue;
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
  public void setPakcetId(int packet) {
    packetResolver.setPacketId(packet);
  }

  @Override
  public byte getNextPacketId() {
    return (byte) packetResolver.incrementPacketIdAndGet();
  }


  @Override
  public MySQLPacketSplitter packetSplitter() {
    return packetSplitter;
  }

  @Override
  public String getLastMessage() {
    String lastMessage = this.serverStatus.getLastMessage();
    return " " + lastMessage + "";
  }

  public String setLastMessage(String lastMessage) {
    this.serverStatus.setLastMessage(lastMessage);
    return lastMessage;
  }

  @Override
  public long affectedRows() {
    return this.serverStatus.getAffectedRows();
  }

  @Override
  public long incrementAffectedRows() {
    return serverStatus.incrementAffectedRows();
  }

  @Override
  public int getServerStatus() {
    return this.serverStatus.getServerStatus();
  }

  @Override
  public int setServerStatus(int s) {
    return this.serverStatus.setServerStatus(s);
  }


  @Override
  public int incrementWarningCount() {
    return this.serverStatus.incrementWarningCount();
  }


  @Override
  public int setLastInsertId(int s) {
    return this.serverStatus.getWarningCount();
  }

  @Override
  public int getLastErrorCode() {
    return this.serverStatus.getWarningCount();
  }

  @Override
  public boolean isDeprecateEOF() {
    return MySQLServerCapabilityFlags.isDeprecateEOF(this.serverStatus.getServerCapabilities());
  }

  public int getWarningCount() {
    return this.serverStatus.getWarningCount();
  }

  public void setWarningCount(int warningCount) {
    this.serverStatus.setWarningCount(warningCount);
  }

  public long getLastInsertId() {
    return this.serverStatus.getLastInsertId();
  }

  public void setLastInsertId(long lastInsertId) {
    this.serverStatus.setLastInsertId(lastInsertId);
  }

  public void resetSession() {
    throw new MycatExpection("unsupport!");
  }

  @Override
  public Charset charset() {
    return this.serverStatus.getCharset();
  }

  public String getCharsetName() {
    return this.serverStatus.getCharsetName();
  }

  @Override
  public int charsetIndex() {
    return this.serverStatus.getCharsetIndex();
  }

  @Override
  public int getCapabilities() {
    return this.serverStatus.getServerCapabilities();
  }

  @Override
  public void setLastErrorCode(int errorCode) {
    this.serverStatus.setLastErrorCode(errorCode);
  }

  @Override
  public boolean isResponseFinished() {
    return responseFinished;
  }

  @Override
  public void setResponseFinished(boolean b) {
    responseFinished = b;
  }

  @Override
  public void switchMySQLServerWriteHandler() {
    this.writeHandler = MySQLProxyServerSession.WriteHandler.INSTANCE;
  }

  @Override
  public void writeToChannel() throws IOException {
    writeHandler.writeToChannel(this);
  }

  public boolean readProxyPayloadFully() {
    return packetResolver.readMySQLPayloadFully();
  }

  @Override
  public final boolean readFromChannel() throws IOException {
    boolean b = MySQLProxySession.super.readFromChannel();
    if (b) {
      MycatMonitor.onFrontRead(this, proxyBuffer.currentByteBuffer(),
          proxyBuffer.channelReadStartIndex(), proxyBuffer.channelReadEndIndex());
    }
    return b;
  }

  public MySQLPacket currentProxyPayload() {
    return packetResolver.currentPayload();
  }

  public void resetCurrentProxyPayload() {
    packetResolver.resetPayload();
  }

  public void resetPacket() {
    packetResolver.reset();
    packetResolver.setState(ComQueryState.QUERY_PACKET);
    for (ByteBuffer byteBuffer : writeQueue) {
      this.getIOThread().getBufPool().recycle(byteBuffer);
    }
    writeQueue.clear();
  }

  @Override
  public long getSelectLimit() {
    return serverStatus.getSelectLimit();
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
    return this.serverStatus.getLocalInFileRequestState();
  }

  @Override
  public void setHandleContentOfFilename(boolean need) {
    this.serverStatus.setLocalInFileRequestState(need);
  }


  @Override
  public void switchNioHandler(NIOHandler nioHandler) {
    this.nioHandler = nioHandler;
  }


  public String getSchema() {
    return schema;
  }


  public void useSchema(String schema) {
    this.schema = schema;
  }


  public boolean hasResultset() {
    return resultSetCount > 0;
  }


  public boolean hasCursor() {
    return false;
  }


  public void countDownResultSet() {
    resultSetCount--;
  }


  public void setResultSetCount(int count) {
    resultSetCount = count;
  }


  public byte[] encode(String text) {
    return text.getBytes(charset());
  }

  public MycatUser getUser() {
    return user;
  }

  public void setUser(MycatUser user) {
    this.user = user;
  }

  public void setCharset(int index) {
    this.setCharset(CharsetUtil.getCharset(index));
  }

  public String getCharacterSetResults() {
    return this.serverStatus.getCharsetSetResult();
  }

  public MycatSessionWriteHandler getWriteHandler() {
    return writeHandler;
  }

  public void setSelectLimit(long sqlSelectLimit) {
    this.serverStatus.setSelectLimit(sqlSelectLimit);
  }

  public void setNetWriteTimeout(long netWriteTimeout) {
    this.serverStatus.setNetWriteTimeout(netWriteTimeout);
  }

  /**
   * 在业务线程使用,在业务线程运行的时候设置业务线程当前的session,方便监听类获取session记录
   * @param thread
   */
  public void deliverWorkerThread(ReactorEnvThread thread) {
    crossSwapThreadBufferPool.bindSource(thread);
    assert thread == Thread.currentThread();
    thread.getReactorEnv().setCurSession(this);
  }

  /**
   * 业务线程执行结束,清除业务线程的session,并代表处理结束
   * @param thread
   */
  public void backFromWorkerThread(ReactorEnvThread thread) {
    crossSwapThreadBufferPool.unbindSource(thread);
    assert thread == Thread.currentThread();
    thread.getReactorEnv().setCurSession(null);
  }
}

package io.mycat.proxy.session;

import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.buffer.BufferPool;
import io.mycat.MySQLPacketUtil;
import io.mycat.proxy.handler.MycatSessionWriteHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author jamie12221 date 2019-05-08 00:06
 * <p>
 * mysql server session 该接口实现服务器模式
 **/
public interface MySQLProxyServerSession<T extends Session<T>> extends MySQLServerSession<T>, Session<T> {
    static final Logger LOGGER = LoggerFactory.getLogger(MySQLProxyServerSession.class);

    BufferPool writeBufferPool();

    /**
     * 前端写入队列
     * @return
     */
    ConcurrentLinkedQueue<ByteBuffer> writeQueue();

    /**
     * mysql 报文头 辅助buffer
     */
    ByteBuffer packetHeaderBuffer();

    /**
     * mysql 报文辅助buffer
     */
    ByteBuffer[] packetContainer();


    /**
     * 前端写入处理器可能有多种,此为设置服务器模式
     */
    void switchMySQLServerWriteHandler();

    MycatReactorThread getIOThread();

    /**
     * 写入payload
     */
    default void writeBytes(byte[] payload, boolean end) {
        try {
            switchMySQLServerWriteHandler();
            ConcurrentLinkedQueue<ByteBuffer> byteBuffers = writeQueue();
            byte[] bytes = MySQLPacketUtil.generateMySQLPacket(getNextPacketId(), payload);
            byteBuffers.offer(writeBufferPool().allocate(bytes));
            change2WriteOpts();
            setResponseFinished(end ? ProcessState.DONE : ProcessState.DOING);
            getIOThread().wakeup();
        } catch (Exception e) {
            this.close(false, setLastMessage(e));
        }
    }

    default void writeErrorEndPacketBySyncInProcessError() {
        writeErrorEndPacketBySyncInProcessError(MySQLErrorCode.ER_UNKNOWN_ERROR);
    }

    default void writeErrorEndPacketBySyncInProcessError(int errorCode) {
        writeErrorEndPacketBySyncInProcessError(1, errorCode);
    }

    /**
     * 同步写入错误包,用于异常处理,一般错误包比较小,一次非阻塞写入就结束了,写入不完整尝试四次, 之后就会把mycat session关闭,简化错误处理
     */
    default void writeErrorEndPacketBySyncInProcessError(int packetId, int errorCode) {
        try {
            setLastErrorCode(errorCode);
            switchMySQLServerWriteHandler();
            this.setResponseFinished(ProcessState.DONE);
            byte[] bytes = MySQLPacketUtil
                    .generateError(errorCode, getLastMessage(),
                            this.getCapabilities());
            byte[] bytes1 = MySQLPacketUtil.generateMySQLPacket(packetId, bytes);
            ByteBuffer message = ByteBuffer.wrap(bytes1);
            int counter = 0;
            SocketChannel channel = channel();
            if (channel.isOpen()) {
                while (message.hasRemaining() && counter < 4) {
                    channel().write(message);
                    counter++;
                }
            }
            if (counter >= 4) {
                this.close(false, "can not response data");
            }
        } catch (IOException e) {
            LOGGER.error("", e);
        } finally {
            close(false, "writeErrorEndPacketBySyncInProcessError");
        }
    }

    MySQLPacketSplitter packetSplitter();


    public void switchProxyWriteHandler();

    /**
     * 前端写入处理器
     */
    public static enum WriteHandler implements MycatSessionWriteHandler {
        INSTANCE;

        @Override
        public void writeToChannel(MycatSession session) throws IOException {
            try {
                if (session.getIOThread() != Thread.currentThread()) {
                    throw new AssertionError();
                }
                Queue<ByteBuffer> byteBuffers = session.writeQueue();
                writeMySQLPacket(session, byteBuffers);
                if (byteBuffers.isEmpty() && session.getProcessState() == ProcessState.DONE) {
                    session.writeFinished(session);
                    session.change2ReadOpts();
                    return;
                } else {
                    session.change2WriteOpts();
                    return;
                }
            } catch (Exception e) {
                onException(session, e);
                throw e;
            }
        }


        @Override
        public void onException(MycatSession session, Exception e) {
            MycatMonitor.onMycatServerWriteException(session, e);
            session.resetPacket();
        }

        @Override
        public void onClear(MycatSession session) {
            BufferPool bufPool = session.writeBufferPool();
            for (ByteBuffer byteBuffer : session.writeQueue()) {
                bufPool.recycle(byteBuffer);
            }
            session.writeQueue().clear();
        }

        @Override
        public WriteType getType() {
            return WriteType.SERVER;
        }

    }

    /**
     * @param session
     * @param byteBuffers
     * @throws IOException
     */
    static void writeMySQLPacket(MySQLProxyServerSession session, Queue<ByteBuffer> byteBuffers) throws IOException {
        long writed = 0;
        session.updateLastActiveTime();
        do {
            ByteBuffer buffer = byteBuffers.peek();
            if (buffer!=null){
                writed = session.channel().write(buffer);
                if (!buffer.hasRemaining()){
                    session.writeBufferPool().recycle(buffer);
                    byteBuffers.remove();
                }
            }else{
                break;
            }
        }while (writed>0);
        if (writed == -1) {
            throw new ClosedChannelException();
        }
    }

    /**
     * 生成packet
     */
    static void splitPacket(MySQLProxyServerSession session, ByteBuffer[] packetContainer,
                            MySQLPacketSplitter packetSplitter,
                            ByteBuffer first) {
        int offset = packetSplitter.getOffsetInPacketSplitter();
        int len = packetSplitter.getPacketLenInPacketSplitter();
        setPacketHeader(session, packetContainer, len);

        first.position(offset).limit(len + offset);
        packetContainer[1] = first;
    }

    /**
     * 构造packet header
     */
    static void setPacketHeader(MySQLProxyServerSession session, ByteBuffer[] packetContainer,
                                int len) {
        ByteBuffer header = session.packetHeaderBuffer();
        header.position(0).limit(4);
        MySQLPacket.writeFixIntByteBuffer(header, 3, len);
        byte nextPacketId = session.getNextPacketId();
        header.put(nextPacketId);
        packetContainer[0] = header;
        header.flip();
    }
}

package io.mycat.proxy.session;

import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.buffer.BufferPool;
import io.mycat.MySQLPacketUtil;
import io.mycat.proxy.handler.MycatSessionWriteHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.util.VertxUtil;
import io.vertx.core.impl.future.PromiseImpl;
import io.vertx.core.impl.future.PromiseInternal;
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
     *
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
    @Override
    MycatReactorThread getIOThread();

    /**
     * 写入payload
     */
    @Override
    default PromiseInternal<Void> writeBytes(byte[] payload, boolean end) {
        try {
            switchMySQLServerWriteHandler();
            ConcurrentLinkedQueue<ByteBuffer> byteBuffers = writeQueue();
            byte[] bytes = MySQLPacketUtil.generateMySQLPacket(getNextPacketId(), payload);
            byteBuffers.offer(writeBufferPool().allocate(bytes));
            change2WriteOpts();

            setResponseFinished(end ? ProcessState.DONE : ProcessState.DOING);
            getIOThread().wakeup();
            return VertxUtil.newSuccessPromise();
        } catch (Exception e) {
            this.close(false, setLastMessage(e));
            return VertxUtil.newFailPromise(e);
        }
    }
    @Override
    default PromiseInternal<Void> writeErrorEndPacketBySyncInProcessError() {
        return writeErrorEndPacketBySyncInProcessError(MySQLErrorCode.ER_UNKNOWN_ERROR);
    }

//    default void writeErrorEndPacketBySyncInProcessError(int errorCode) {
//        writeErrorEndPacketBySyncInProcessError(1, errorCode);
//    }

    /**
     * 同步写入错误包,用于异常处理,一般错误包比较小,一次非阻塞写入就结束了,写入不完整尝试四次, 之后就会把mycat session关闭,简化错误处理
     */
    @Override
    default PromiseInternal<Void> writeErrorEndPacketBySyncInProcessError( int errorCode) {
        if (channel().isConnected()){
            setLastErrorCode(errorCode);
            switchMySQLServerWriteHandler();
            byte[] bytes = MySQLPacketUtil
                    .generateError(errorCode, getLastMessage(),
                            this.getCapabilities());
            writeBytes( bytes, true);
        }
        return VertxUtil.newSuccessPromise();
    }

    MySQLPacketSplitter packetSplitter();


    public void switchProxyWriteHandler();

    /**
     * 前端写入处理器
     */
    public static enum WriteHandler implements MycatSessionWriteHandler {
        INSTANCE;

        @Override
        public PromiseInternal<Void>  writeToChannel(MycatSession session) throws IOException {
                if (session.getIOThread() != Thread.currentThread()) {
                    throw new AssertionError();
                }
                Queue<ByteBuffer> byteBuffers = session.writeQueue();
                writeMySQLPacket(session, byteBuffers);
                if (byteBuffers.isEmpty() && session.getProcessState() == ProcessState.DONE) {
                    session.writeFinished(session);
                    session.change2ReadOpts();
                    return VertxUtil.newSuccessPromise();
                } else {
                    session.change2WriteOpts();
                    // todo 异步未实现完全 wangzihaogithub
                    return VertxUtil.newSuccessPromise();
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
            if (buffer != null) {
                try {
                    writed = session.channel().write(buffer);
                }catch (Throwable throwable){
                    LOGGER.error("",throwable);
                    throw new ClosedChannelException();
                }
                if (!buffer.hasRemaining()) {
                    session.writeBufferPool().recycle(buffer);
                    byteBuffers.remove();
                }
            } else {
                break;
            }
        } while (writed > 0);
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

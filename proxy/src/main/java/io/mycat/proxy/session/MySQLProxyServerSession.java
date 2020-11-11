package io.mycat.proxy.session;

import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.buffer.BufferPool;
import io.mycat.MySQLPacketUtil;
import io.mycat.proxy.buffer.CrossSwapThreadBufferPool;
import io.mycat.proxy.handler.MycatSessionWriteHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;

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
     */
    LinkedTransferQueue<ByteBuffer> writeQueue();

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

    default void writeBytes(ByteBuffer buffer, boolean end) {
        //@ ServerTransactionSessionRunner
        try {
            switchMySQLServerWriteHandler();
            Queue<ByteBuffer> byteBuffers = writeQueue();
            byteBuffers.offer(buffer);
            change2WriteOpts();
            setResponseFinished(end ? ProcessState.DONE : ProcessState.DOING);
        } catch (Exception e) {
            this.close(false, setLastMessage(e));
        }
    }

    /**
     * 写入payload
     */
    default void writeBytes(byte[] payload, boolean end) {
        writeBytes(writeBufferPool().allocate(payload), end);
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
            BufferPool bufPool = session.getIOThread().getBufPool();
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
     * @return 有剩余的数据, 没有写入完整
     * @throws IOException
     */
    static boolean writeMySQLPacket(MySQLProxyServerSession session, Queue<ByteBuffer> byteBuffers) throws IOException {
        ByteBuffer[] packetContainer = session.packetContainer();
        MySQLPacketSplitter packetSplitter = session.packetSplitter();
        long writed;
        session.updateLastActiveTime();
        do {
            writed = 0;
            if (byteBuffers.isEmpty()) {
                break;
            }
            ByteBuffer first = byteBuffers.peek();

            if (first.position() == 0) {//一个全新的payload
                MycatMonitor.onFrontWrite(
                        session, first, 0, first.limit());
                packetSplitter.init(first.limit());
                packetSplitter.nextPacketInPacketSplitter();
                splitPacket(session, packetContainer, packetSplitter, first);
                assert packetContainer[0] != null;
                assert packetContainer[1] != null;
                writed = session.channel().write(packetContainer);
                if (first.hasRemaining()) {
                    return true;
                } else {
                    continue;
                }
            } else {
                assert packetContainer[0] != null;
                assert packetContainer[1] != null;
                writed = session.channel().write(packetContainer);
                if (first.hasRemaining()) {
                    return true;
                } else {
                    if (packetSplitter.nextPacketInPacketSplitter()) {
                        splitPacket(session, packetContainer, packetSplitter, first);
                        writed = session.channel().write(packetContainer);
                        if (first.hasRemaining()) {
                            return true;
                        } else {
                            continue;
                        }
                    } else {
                        byteBuffers.remove();
                        session.writeBufferPool().recycle(first);
                    }
                }
            }
        } while (writed > 0);
        if (writed == -1) {
            throw new ClosedChannelException();
        }
        return false;
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

package io.mycat.mycat2;

import io.mycat.mycat2.beans.MySQLCharset;
import io.mycat.mycat2.beans.conf.ProxyConfig;
import io.mycat.mysql.*;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.AbstractSession;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.TimeUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 抽象的MySQL的连接会话
 *
 * @author wuzhihui
 */
public abstract class AbstractMySQLSession extends AbstractSession {



    public AbstractMySQLSession() {

    }

    /**
     * 字符集
     */
    public MySQLCharset charSet = new MySQLCharset();
    /**
     * 用户
     */
    public String clientUser;

    /**
     * 事务隔离级别
     */
    public Isolation isolation = Isolation.REPEATED_READ;

    /**
     * 事务提交方式
     */
    public AutoCommit autoCommit = AutoCommit.ON;

    /**
     * 认证中的seed报文数据
     */
    public byte[] seed;

    protected long lastLargeMessageTime;
    protected long lastReadTime;

    /**
     * 当前处理中的SQL报文的信息
     */
    public final MySQLPacketInf curPacketInf = new MySQLPacketInf();


    /**
     * 用来进行指定结束报文处理
     */
    public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel, NIOHandler nioHandler) throws IOException {
        this(bufferPool, selector, channel, SelectionKey.OP_READ,nioHandler);

    }


    public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel, int keyOpt, NIOHandler nioHandler)
            throws IOException {
        super(bufferPool, selector, channel, keyOpt,nioHandler);

    }

    public void setCurBufOwner(boolean curBufOwner) {
        this.curBufOwner = curBufOwner;
    }

    public PayloadType resolveFullPayload() {
        return this.curPacketInf.resolveFullPayload(this.proxyBuffer);
    }

    /**
     * 回应客户端（front或Sever）OK 报文。
     *
     * @param pkg ，必须要是OK报文或者Err报文
     * @throws IOException
     */
    public void responseOKOrError(MySQLPacket pkg) {
        // proxyBuffer.changeOwner(true);
        this.proxyBuffer.reset();
        pkg.write(this.proxyBuffer);
        // 设置frontBuffer 为读取状态
        proxyBuffer.flip();
        proxyBuffer.readIndex = proxyBuffer.writeIndex;

        try {
            this.writeToChannel();
        } catch (Exception e) {
            logger.error("response write err , {} ", e);
        }
    }

    /**
     * 回应客户端（front或Sever）OK 报文。
     *
     * @param pkg ，必须要是OK报文或者Err报文
     * @throws IOException
     */
    public void responseOKOrError(byte[] pkg) throws IOException {
        // proxyBuffer.changeOwner(true);
        this.proxyBuffer.reset();
        proxyBuffer.writeBytes(pkg);
        proxyBuffer.flip();
        proxyBuffer.readIndex = proxyBuffer.writeIndex;
        this.writeToChannel();
    }


    public void ensureFreeSpaceOfReadBuffer() {
        int pkgLength = curPacketInf.pkgLength;
        ByteBuffer buffer = proxyBuffer.getBuffer();
        ProxyConfig config = ProxyRuntime.INSTANCE.getConfig().getConfig(ConfigEnum.PROXY);
        // need a large buffer to hold the package
        if (pkgLength > config.getProxy().getMax_allowed_packet()) {
            throw new IllegalArgumentException("Packet size over the limit.");
        } else if (buffer.capacity() < pkgLength) {
            logger.debug("need a large buffer to hold the package.{}", curPacketInf);
            lastLargeMessageTime = TimeUtil.currentTimeMillis();
            MySQLProxyPacketResolver.simpleAdjustCapacityProxybuffer(proxyBuffer,proxyBuffer.writeIndex+pkgLength);
//            ByteBuffer newBuffer = bufPool.allocate(Double.valueOf(pkgLength + pkgLength * 0.1).intValue());
//            resetBuffer(newBuffer);
        } else {
            if (proxyBuffer.writeIndex != 0) {
                // compact bytebuffer only
                proxyBuffer.compact();
            } else {
              //  throw new RuntimeException(" not enough space");
            }
        }
    }

    /**
     * 重置buffer
     *
     * @param newBuffer
     */
    private void resetBuffer(ByteBuffer newBuffer) {
        newBuffer.put(proxyBuffer.getBytes(proxyBuffer.readIndex, proxyBuffer.writeIndex - proxyBuffer.readIndex));
        proxyBuffer.resetBuffer(newBuffer);
        recycleAllocedBuffer(proxyBuffer);
        curPacketInf.endPos = curPacketInf.endPos - curPacketInf.startPos;
        curPacketInf.startPos = 0;
    }

    /**
     * 检查 是否需要切换回正常大小buffer.
     */
    public void changeToDirectIfNeed() {

        if (!proxyBuffer.getBuffer().isDirect()) {

            if (curPacketInf.pkgLength > bufPool.getChunkSize()) {
                lastLargeMessageTime = TimeUtil.currentTimeMillis();
                return;
            }

            if (lastLargeMessageTime < lastReadTime - 30 * 1000L) {
                logger.info("change to direct con read buffer ,cur temp buf size : {}",
                        proxyBuffer.getBuffer().capacity());
                ByteBuffer bytebuffer = bufPool.allocate();
                if (!bytebuffer.isDirect()) {
                    bufPool.recycle(bytebuffer);
                } else {
                    resetBuffer(bytebuffer);
                }
                lastLargeMessageTime = TimeUtil.currentTimeMillis();
            }
        }
    }

}

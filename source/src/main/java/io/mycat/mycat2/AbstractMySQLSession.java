package io.mycat.mycat2;

import io.mycat.mycat2.beans.MySQLCharset;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.beans.conf.ProxyConfig;
import io.mycat.mycat2.console.SessionKey;
import io.mycat.mysql.AutoCommit;
import io.mycat.mysql.Isolation;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.AbstractSession;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ParseUtil;
import io.mycat.util.StringUtil;
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

    // 当前接收到的包类型
    public enum CurrPacketType {
        Full, LongHalfPacket, ShortHalfPacket
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
    public MySQLPackageInf curMSQLPackgInf = new MySQLPackageInf();

    /**
     * 标识当前连接的闲置状态标识 ，true，闲置，false，未闲置,即在使用中
     */
    boolean idleFlag = true;

    /**
     * 用来进行指定结束报文处理
     */
    //  public CommandHandler commandHandler = CommQueryHandler.INSTANCE;
    public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
        this(bufferPool, selector, channel, SelectionKey.OP_READ);

    }

    public void setIdle() {
        if(logger.isDebugEnabled()){
            logger.debug("mysql session:{} is idle",this);
        }
        this.idleFlag = true;
    }

    public void setBusy() {
        this.idleFlag = false;
    }

    public boolean isIdle() {
        return this.idleFlag;
    }

    public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel, int keyOpt)
            throws IOException {
        super(bufferPool, selector, channel, keyOpt);

    }

    public void setCurBufOwner(boolean curBufOwner) {
        this.curBufOwner = curBufOwner;
    }

    /**
     * 回应客户端（front或Sever）OK 报文。
     *
     * @param pkg ，必须要是OK报文或者Err报文
     * @throws IOException
     */
    public void responseOKOrError(MySQLPacket pkg) throws IOException {
        // proxyBuffer.changeOwner(true);
        this.proxyBuffer.reset();
        pkg.write(this.proxyBuffer);
        proxyBuffer.flip();
        proxyBuffer.readIndex = proxyBuffer.writeIndex;
        this.writeToChannel();
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
        proxyBuffer.writeBytes(OKPacket.OK);
        proxyBuffer.flip();
        proxyBuffer.readIndex = proxyBuffer.writeIndex;
        this.writeToChannel();
    }

    public CurrPacketType resolveMySQLPackage() throws IOException {
        return resolveMySQLPackage(proxyBuffer, curMSQLPackgInf, true);
    }

    /**
     * 解析MySQL报文，解析的结果存储在curMSQLPackgInf中，如果解析到完整的报文，就返回TRUE
     * 如果解析的过程中同时要移动ProxyBuffer的readState位置，即标记为读过，后继调用开始解析下一个报文，则需要参数markReaded
     * =true
     *
     * @param proxyBuf
     * @return
     * @throws IOException
     */
    public CurrPacketType resolveMySQLPackage(ProxyBuffer proxyBuf, MySQLPackageInf curPackInf, boolean markReaded) throws IOException {

        lastReadTime = TimeUtil.currentTimeMillis();

        ByteBuffer buffer = proxyBuf.getBuffer();
        // 读取的偏移位置
        int offset = proxyBuf.readIndex;
        // 读取的总长度
        int limit = proxyBuf.writeIndex;
        // 读取当前的总长度
        int totalLen = limit - offset;
        if (totalLen == 0) { // 透传情况下. 如果最后一个报文正好在buffer 最后位置,已经透传出去了.这里可能不会为零
            return CurrPacketType.ShortHalfPacket;
        }

        if (curPackInf.remainsBytes == 0 && curPackInf.crossBuffer) {
            curPackInf.crossBuffer = false;
        }

        // 如果当前跨多个报文
        if (curPackInf.crossBuffer) {
            if (curPackInf.remainsBytes <= totalLen) {
                // 剩余报文结束
                curPackInf.endPos = offset + curPackInf.remainsBytes;
                offset += curPackInf.remainsBytes; // 继续处理下一个报文
                proxyBuf.readIndex = offset;
                curPackInf.remainsBytes = 0;
            } else {// 剩余报文还没读完，等待下一次读取
                curPackInf.startPos = 0;
                curPackInf.remainsBytes -= totalLen;
                curPackInf.endPos = limit;
                proxyBuf.readIndex = curPackInf.endPos;
                return CurrPacketType.LongHalfPacket;
            }
        }
        // 验证当前指针位置是否
        if (!ParseUtil.validateHeader(offset, limit)) {
            // 收到短半包
            logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset, limit);
            return CurrPacketType.ShortHalfPacket;
        }

        // 解包获取包的数据长度
        int pkgLength = ParseUtil.getPacketLength(buffer, offset);

        // 解析报文类型
        int packetType = -1;

        // 在包长度小于7时，作为resultSet的首包
        if (pkgLength <= 7) {
            int index = offset + ParseUtil.msyql_packetHeaderSize;

            long len = proxyBuf.getInt(index, 1) & 0xff;
            // 如果长度小于251,则取默认的长度
            if (len < 251) {
                packetType = (int) len;
            } else if (len == 0xfc) {
                // 进行验证是否位数足够,作为短包处理
                if (!ParseUtil.validateResultHeader(offset, limit, 2)) {
                    // 收到短半包
                    logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset,
                            limit);
                    return CurrPacketType.ShortHalfPacket;
                }
                packetType = (int) proxyBuf.getInt(index + 1, 2);
            } else if (len == 0xfd) {

                // 进行验证是否位数足够,作为短包处理
                if (!ParseUtil.validateResultHeader(offset, limit, 3)) {
                    // 收到短半包
                    logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset,
                            limit);
                    return CurrPacketType.ShortHalfPacket;
                }

                packetType = (int) proxyBuf.getInt(index + 1, 3);
            } else {
                // 进行验证是否位数足够,作为短包处理
                if (!ParseUtil.validateResultHeader(offset, limit, 8)) {
                    // 收到短半包
                    logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset,
                            limit);
                    return CurrPacketType.ShortHalfPacket;
                }

                packetType = (int) proxyBuf.getInt(index + 1, 8);
            }
        } else {
            // 解析报文类型
            packetType = buffer.get(offset + ParseUtil.msyql_packetHeaderSize);
        }

        // 包的类型
        curPackInf.pkgType = packetType;
        // 设置包的长度
        curPackInf.pkgLength = pkgLength;
        // 设置偏移位置
        curPackInf.startPos = offset;

        curPackInf.crossBuffer = false;

        curPackInf.remainsBytes = 0;
        // 如果当前需要跨buffer处理


        if ((offset + pkgLength) > limit) {
            logger.debug("Not a whole packet: required length = {} bytes, cur total length = {} bytes, limit ={}, "
                    + "ready to handle the next read event", pkgLength, (limit - offset), limit);
            if (offset == 0 && pkgLength > limit) {
                /*
                cjw 2018.4.6
                假设整个buffer空间为88,开始位置是0,需要容纳89的数据大小,还缺一个数据没用接受完,
                之后作为LongHalfPacket返回,之后上一级处理结果的函数因为是解析所以只处理整包,之后就一直不处理数据,
                导致一直没有把数据处理,一直报错 readed zero bytes ,Maybe a bug ,please fix it !!!!
                解决办法:扩容
                 */
                proxyBuf.setBuffer(this.bufPool.expandBuffer(this.proxyBuffer.getBuffer()));
            }
            curPackInf.endPos = limit;
            return CurrPacketType.LongHalfPacket;
        } else {
            // 读到完整报文
            curPackInf.endPos = curPackInf.pkgLength + curPackInf.startPos;
            if (ProxyRuntime.INSTANCE.isTraceProtocol()) {
                /**
                 * @todo 跨多个报文的情况下，修正错误。
                 */
                final String hexs = StringUtil.dumpAsHex(buffer, curPackInf.startPos, curPackInf.pkgLength);
                logger.debug(
                        "     session {} packet: startPos={}, offset = {}, length = {}, type = {}, cur total length = {},pkg HEX\r\n {}",
                        getSessionId(), curPackInf.startPos, offset, pkgLength, packetType, limit, hexs);
            }
            if (markReaded) {
                proxyBuf.readIndex = curPackInf.endPos;
            }
            return CurrPacketType.Full;
        }
    }

    public void ensureFreeSpaceOfReadBuffer() {
        int pkgLength = curMSQLPackgInf.pkgLength;
        ByteBuffer buffer = proxyBuffer.getBuffer();
        ProxyConfig config = ProxyRuntime.INSTANCE.getConfig().getConfig(ConfigEnum.PROXY);
        // need a large buffer to hold the package
        if (pkgLength > config.getProxy().getMax_allowed_packet()) {
            throw new IllegalArgumentException("Packet size over the limit.");
        } else if (buffer.capacity() < pkgLength) {
            logger.debug("need a large buffer to hold the package.{}", curMSQLPackgInf);
            lastLargeMessageTime = TimeUtil.currentTimeMillis();
            ByteBuffer newBuffer = bufPool.allocate(Double.valueOf(pkgLength + pkgLength * 0.1).intValue());
            resetBuffer(newBuffer);
        } else {
            if (proxyBuffer.writeIndex != 0) {
                // compact bytebuffer only
                proxyBuffer.compact();
            } else {
                throw new RuntimeException(" not enough space");
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
        curMSQLPackgInf.endPos = curMSQLPackgInf.endPos - curMSQLPackgInf.startPos;
        curMSQLPackgInf.startPos = 0;
    }

    /**
     * 检查 是否需要切换回正常大小buffer.
     */
    public void changeToDirectIfNeed() {

        if (!proxyBuffer.getBuffer().isDirect()) {

            if (curMSQLPackgInf.pkgLength > bufPool.getChunkSize()) {
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

    public int getPkgType() {
        return (Integer) this.getAttrMap().get(SessionKey.PKG_TYPE_KEY);
    }

    public void setPkgType(int value) {
        this.getAttrMap().put(SessionKey.PKG_TYPE_KEY, value);
    }

    public boolean isTrans() {
        return this.getAttrMap().containsKey(SessionKey.TRANSACTION_FLAG);
    }

    public void setTrans(boolean value) {
        if (value) {
            this.getAttrMap().put(SessionKey.TRANSACTION_FLAG, true);
        } else {
            this.getAttrMap().remove(SessionKey.TRANSACTION_FLAG);
        }
    }

    public void removePkgReadFlag() {
        this.getAttrMap().remove(SessionKey.PKG_READ_FLAG);
    }

    public boolean isPkgReadFlag() {
        return this.getAttrMap().containsKey(SessionKey.PKG_READ_FLAG);
    }

    public void setPkgReadFlag() {
        this.getAttrMap().put(SessionKey.TRANSACTION_FLAG, true);
    }

}

package io.mycat.mysql;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

/*
cjw
294712221@qq.com
 */
public class MySQLPacketInf {
    private final static Logger logger = LoggerFactory.getLogger(MySQLPacketInf.class);
    public int head;
    public int startPos;
    public int endPos;
    public int pkgLength;
    public int remainsBytes;//还有多少字节才结束，仅对跨多个Buffer的MySQL报文有意义（crossBuffer=true)
    public PacketType packetType = PacketType.SHORT_HALF;
    public ProxyBuffer proxyBuffer;
    public MySQLProxyPacketResolver resolver = new MySQLProxyPacketResolver();
    public ByteBuffer largePayload;
    public BufferPool bufferPool;

    public static boolean resolveFullPayloadExpendBuffer(MycatSession session) {
        MySQLPacketInf inf = session.curPacketInf;
        inf.resolver.resolveMySQLPacket(inf);
        int startPos = inf.startPos + 4;
        int endPos = inf.endPos;
        int length = endPos - startPos;
        while (true) {
            switch (inf.packetType) {
                case FULL: {
                    if (inf.resolver.crossPacket) {
                        if (inf.largePayload == null) {//判断是否第一个报文,所以一定要回收inf.largeBuffer表示结束
                            inf.bufferPool = session.bufPool;
                            inf.largePayload = session.bufPool.allocate(length);
                        } else if ((inf.largePayload.limit() - inf.largePayload.position()) <= (length + inf.largePayload.position())) {
                            inf.bufferPool = session.bufPool;
                            inf.largePayload = session.bufPool.expandBuffer(inf.largePayload, length);
                        }
                        inf.largePayload.clear();
                        ByteBuffer source = inf.proxyBuffer.getBuffer().duplicate();
                        source.position(startPos).limit(endPos);
                        inf.largePayload.put(source);
                        inf.markRead();
                        return false;
                    } else {
                        if (inf.largePayload == null) {//判断是否第一个报文
                            //如果第一个报文满足条件则直接使用proxybuffer的bytebuffer作为报文内容,减少拷贝
                            inf.largePayload = inf.proxyBuffer.getBuffer().duplicate();
                            inf.largePayload.position(startPos);
                            inf.largePayload.limit(endPos);
                            inf.markRead();
                            return true;
                        } else {
                            ByteBuffer source = inf.proxyBuffer.getBuffer().duplicate();
                            source.position(startPos).limit(endPos);
                            inf.largePayload.put(source);
                            int finalLength = inf.largePayload.position();
                            inf.largePayload.position(0);
                            inf.largePayload.position(finalLength);
                            inf.markRead();
                            return true;
                        }

                    }
                }
                default:
                    session.ensureFreeSpaceOfReadBuffer();
                    return false;
            }
        }

    }
    public static MySQLPacketInf directPassthrouhBuffer(MySQLSession mySQLSession) {
        MySQLPacketInf packetInf = mySQLSession.curPacketInf;
        packetInf.proxyBuffer = mySQLSession.proxyBuffer;
        boolean loop = true;
        while (	loop) {
            PayloadType payloadType = packetInf.resolveCrossBufferMySQLPayload(mySQLSession.proxyBuffer);
            switch (payloadType) {
                case SHORT_PAYLOAD:
                case LONG_PAYLOAD:
                case REST_CROSS_PAYLOAD:
                    loop = false;
                    if (packetInf.resolver.state.isNeedFull()){
                        MySQLProxyPacketResolver.simpleAdjustCapacityProxybuffer(packetInf.proxyBuffer,packetInf.endPos+packetInf.pkgLength);
                    }
                    break;
                case FULL_PAYLOAD:
                case FINISHED_CROSS_PAYLOAD:
                    loop = true;
                    break;
            }
        }
        return packetInf;
    }
    public void recycleLargePayloadBuffer() {
        if (bufferPool != null && largePayload != null) {
            bufferPool.recycle(largePayload);
            this.bufferPool = null;
        }
        this.largePayload = null;//proxybuffer or bufferPool
    }

    public boolean needContinueResolveMySQLPacket() {
        return proxyBuffer.readIndex < proxyBuffer.writeIndex;
    }

    public MySQLPayloadType getType() {
        return resolver.mysqlPacketType;
    }

    public ComQueryState getState() {
        return resolver.state;
    }

    public int getCurrPacketId() {
        return resolver.nextPacketId - 1;
    }

    public PayloadType resolveCrossBufferMySQLPayload(ProxyBuffer proxyBuffer) {
        this.proxyBuffer = proxyBuffer;
        return resolver.resolveCrossBufferFullPayload(this);
    }

    public PayloadType resolveFullPayload(ProxyBuffer proxyBuffer) {
        this.proxyBuffer = proxyBuffer;
        return resolver.resolveFullPayload(this);
    }

    public void shift2DefRespPacket() {
        resolver.shift2DefRespPacket();
    }

    public void shift2DefQueryPacket() {
        resolver.shift2DefQueryPacket();
    }

    public void shift2QueryPacket() {
        resolver.shift2QueryPacket();
    }

    public void shift2RespPacket() {
        resolver.shift2RespPacket();
    }

    public boolean needContinueOnReadingRequest() {
        return this.resolver.needContinueOnReadingRequest();
    }

    public boolean isResponseFinished() {
        return this.resolver.isResponseFinished();
    }

    public boolean isInteractive() {
        return this.resolver.isInteractive();
    }

    public void updateState(
            int head,
            int startPos,
            int endPos,
            int pkgLength,
            int remainsBytes,
            PacketType packetType,
            ProxyBuffer proxyBuffer
    ) {
        String oldState = null;
//        if (logger.isDebugEnabled()) {
//            oldState = this.toString();
//            logger.debug("from {}", oldState);
//        }
        this.head = head;
        this.startPos = startPos;
        this.endPos = endPos;
        this.pkgLength = pkgLength;
        this.remainsBytes = remainsBytes;
        this.packetType = packetType;
        this.proxyBuffer = proxyBuffer;
//        if (logger.isDebugEnabled()) {
//            logger.debug("to   {}", this.toString());
//        }
    }

    public MySQLPacketInf(ProxyBuffer proxyBuffer) {
        this.proxyBuffer = proxyBuffer;
    }

    public MySQLPacketInf() {
    }

    public boolean needExpandBuffer() {
        return pkgLength > proxyBuffer.getBuffer().capacity();
    }

    public PacketType change2ShortHalf() {
        this.updateState(0, 0, 0, 0, 0, PacketType.SHORT_HALF, proxyBuffer);
        return PacketType.SHORT_HALF;
    }


    public void markRead() {
        if (packetType == PacketType.FULL || packetType == PacketType.REST_CROSS || packetType == PacketType.FINISHED_CROSS) {
            this.proxyBuffer.readIndex = endPos;
        } else {
            throw new UnsupportedOperationException("markRead is only in FULL or REST_CROSS or FINISHED_CROSS");
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MySQLPacketInf.class.getSimpleName() + "[", "]")
                .add("head=" + head)
                .add("startPos=" + startPos)
                .add("endPos=" + endPos)
                .add("pkgLength=" + pkgLength)
                .add("remainsBytes=" + remainsBytes)
                .add("packetType=" + packetType)
                .add("proxyBuffer=" + proxyBuffer)
                .toString();
    }
}

package io.mycat.mysql;

import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public boolean needContinueResolveMySQLPacket() {
        return proxyBuffer.readIndex != proxyBuffer.writeIndex;
    }
    public MySQLPayloadType getType() {
        return resolver.mysqlPacketType;
    }
    public int getCurrPacketId() {
        return resolver.nextPacketId-1;
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

    public boolean isCommandFinished() {
        return this.resolver.isCommandFinished();
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
        if (logger.isDebugEnabled()) {
            oldState = this.toString();
            logger.debug("from {}", oldState);
        }
        this.head = head;
        this.startPos = startPos;
        this.endPos = endPos;
        this.pkgLength = pkgLength;
        this.remainsBytes = remainsBytes;
        this.packetType = packetType;
        this.proxyBuffer = proxyBuffer;
        if (logger.isDebugEnabled()) {
            logger.debug("to   {}", this.toString());
        }
    }

    public MySQLPacketInf(ProxyBuffer proxyBuffer) {
        this.proxyBuffer = proxyBuffer;
    }

    public MySQLPacketInf() {
    }

    public boolean needExpandBuffer() {
        return startPos == 0 && pkgLength > endPos && pkgLength > proxyBuffer.getBuffer().capacity();
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

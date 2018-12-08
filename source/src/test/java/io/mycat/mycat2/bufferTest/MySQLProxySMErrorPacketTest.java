package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.testTool.TestUtil;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.mycat.mycat2.bufferTest.MySQLRespPacketType.UNKNOWN;

public class MySQLProxySMErrorPacketTest {

    private static int HEADER_SIZE = 4;

    @Test
    public void fullErrorOnePacket() {
        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        errorPacket.write(buffer);

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(MySQLRespPacketType.ERROR, sm.mysqlPacketType);
        Assert.assertEquals(HEADER_SIZE + errorPacket.calcPacketSize(), packetInf.pkgLength);
        Assert.assertEquals(errorPacket.packetId, sm.lastPacketId);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(packetInf.pkgLength, packetInf.endPos);
        Assert.assertEquals(0xff, packetInf.head);
    }

    @Test
    public void shortErrorOnePacket() {
        ErrorPacket errorPacket = TestUtil.errPacket(1);
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        errorPacket.write(buffer);
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
        for (int i = 0; i < HEADER_SIZE+1; i++) {
            buffer.writeIndex = i;
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, packetInf.packetType);
            Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
        }
    }

    @Test
    public void longHalfOnePacket() {
        ErrorPacket eofPacket = new ErrorPacket();
        eofPacket.packetId = 1;
        eofPacket.message = "";
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        eofPacket.write(buffer);
        int length = buffer.writeIndex;
        for (int i = HEADER_SIZE + 1; i < length; i++) {
            MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
            MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
            buffer.writeIndex = i;
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, packetInf.packetType);
            Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
            Assert.assertEquals(length, packetInf.pkgLength);
            Assert.assertEquals(0, packetInf.startPos);
            Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
            Assert.assertEquals(0xff, packetInf.head);
        }
    }

    @Test
    public void longHalf2Full2ShortOnePacket() {
        ErrorPacket eofPacket = new ErrorPacket();
        eofPacket.packetId = 2;
        eofPacket.message = "";
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        eofPacket.write(buffer);

        int length = buffer.writeIndex;

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 1;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
        buffer.writeIndex = length - 1;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, packetInf.packetType);
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
        Assert.assertEquals(0xff, packetInf.head);

        buffer.writeIndex = length;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(MySQLRespPacketType.ERROR, sm.mysqlPacketType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
        Assert.assertEquals(0xff, packetInf.head);

        packetInf.markRead();

        eofPacket.packetId = 2;
        eofPacket.write(buffer);
        buffer.writeIndex = length + 1;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, packetInf.packetType);
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
    }

    @Test
    public void longHalf2RestCross2Finished2ShortOnePacket() {
        ErrorPacket eofPacket = new ErrorPacket();
        eofPacket.packetId = 1;
        eofPacket.message = "";
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        eofPacket.write(buffer);
        int length = buffer.writeIndex;

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        packetInf.proxyBuffer = buffer;
        buffer.writeIndex = HEADER_SIZE + 1;//longHalf
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, packetInf.packetType);
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
        Assert.assertEquals(0xff, packetInf.head);

        sm.crossBuffer(packetInf);

        for (int i = buffer.writeIndex; i < length; i++) {
            buffer.writeIndex = i;
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, packetInf.packetType);
            Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
            Assert.assertEquals(length, packetInf.pkgLength);
            Assert.assertEquals(0, packetInf.startPos);
            Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
            Assert.assertEquals(0xff, packetInf.head);

            Assert.assertEquals(0,packetInf.remainsBytes);//因为crossBuffer失败,一直没有使用剩余长度计算
        }
        buffer.writeIndex++;

        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(MySQLRespPacketType.ERROR, sm.mysqlPacketType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
        Assert.assertEquals(0xff, packetInf.head);

        Assert.assertEquals(0, packetInf.remainsBytes);

        packetInf.markRead();

        eofPacket.packetId = 2;
        eofPacket.write(buffer);
        buffer.writeIndex = length + 1;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, packetInf.packetType);
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
    }

    @Test
    public void fullPayloadOnePacket() {
        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        errorPacket.write(buffer);

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FULL_PAYLOAD, sm.resolveFullPayload(packetInf));
        Assert.assertEquals(MySQLRespPacketType.ERROR, sm.mysqlPacketType);
    }

    @Test
    public void fullPayloadMutilPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;

        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff+8));
        errorPacket.write(buffer);
        buffer.writeIndex = 0;
        buffer.writeFixInt(3,0xffffff);
        buffer.writeByte((byte) 12);
        buffer.writeIndex = 0xffffff;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.LONG_PAYLOAD, sm.resolveFullPayload(packetInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);

        packetInf.markRead();

        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FULL_PAYLOAD, sm.resolveFullPayload(packetInf));
        Assert.assertEquals(MySQLRespPacketType.ERROR, sm.mysqlPacketType);

    }

    @Test
    public void crossPayloadOnePacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;

        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff+4));
        errorPacket.write(buffer);
        buffer.writeIndex = 0;
        buffer.writeFixInt(3,0xffffff);
        buffer.writeByte((byte) 12);
        buffer.writeIndex = 0xffffff;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);

        packetInf.markRead();

        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        Assert.assertEquals(MySQLRespPacketType.ERROR, sm.mysqlPacketType);

    }

    @Test
    public void crossPayloadMutilPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;

        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff+4));
        errorPacket.write(buffer);
        buffer.writeIndex = 0;
        buffer.writeFixInt(3,0xffffff);
        buffer.writeByte((byte) 12);
        buffer.writeIndex = 0xffffff-1;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.SHORT_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);

        buffer.writeIndex = 0xffffff;
        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        Assert.assertEquals(MySQLRespPacketType.UNKNOWN, sm.mysqlPacketType);

        packetInf.markRead();

        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        Assert.assertEquals(MySQLRespPacketType.ERROR, sm.mysqlPacketType);

    }
}

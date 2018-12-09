package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.testTool.TestUtil;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.mysql.MySQLPayloadType;
import io.mycat.mysql.MySQLProxyPacketResolver;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.mycat.mysql.MySQLPayloadType.ERROR;
import static io.mycat.mysql.PacketType.*;
import static io.mycat.mysql.PayloadType.*;

public class MySQLProxySMErrorPacketTest {

    private static int HEADER_SIZE = 4;

    @Test
    public void fullErrorOnePacket() {
        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        errorPacket.write(buffer);

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        Assert.assertEquals(FULL, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(FULL, MySQLPacketInf.packetType);
        Assert.assertEquals(ERROR, sm.mysqlPacketType);
        Assert.assertEquals(HEADER_SIZE + errorPacket.calcPacketSize(), MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(MySQLPacketInf.pkgLength, MySQLPacketInf.endPos);
        Assert.assertEquals(0xff, MySQLPacketInf.head);
    }

    @Test
    public void shortErrorOnePacket() {
        ErrorPacket errorPacket = TestUtil.errPacket(1);
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        errorPacket.write(buffer);
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 1;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
        for (int i = 0; i < HEADER_SIZE+1; i++) {
            buffer.writeIndex = i;
            Assert.assertEquals(SHORT_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
            Assert.assertEquals(SHORT_HALF, MySQLPacketInf.packetType);
            Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);
        }
    }

    @Test
    public void longHalfOnePacket() {
        ErrorPacket eofPacket = new ErrorPacket();
        eofPacket.packetId = 0;
        eofPacket.message = "";
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        eofPacket.write(buffer);
        int length = buffer.writeIndex;
        for (int i = HEADER_SIZE + 1; i < length; i++) {
            MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
            MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
            buffer.writeIndex = i;
            Assert.assertEquals(LONG_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
            Assert.assertEquals(LONG_HALF, MySQLPacketInf.packetType);
            Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);
            Assert.assertEquals(length, MySQLPacketInf.pkgLength);
            Assert.assertEquals(0, MySQLPacketInf.startPos);
            Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);
            Assert.assertEquals(0xff, MySQLPacketInf.head);
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
        sm.nextPacketId = 2;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
        buffer.writeIndex = length - 1;
        Assert.assertEquals(LONG_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(LONG_HALF, MySQLPacketInf.packetType);
        Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);
        Assert.assertEquals(length, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);
        Assert.assertEquals(0xff, MySQLPacketInf.head);

        buffer.writeIndex = length;
        Assert.assertEquals(FULL, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(FULL, MySQLPacketInf.packetType);
        Assert.assertEquals(ERROR, sm.mysqlPacketType);
        Assert.assertEquals(length, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);
        Assert.assertEquals(0xff, MySQLPacketInf.head);

        MySQLPacketInf.markRead();

        eofPacket.packetId = 2;
        eofPacket.write(buffer);
        buffer.writeIndex = length + 1;
        Assert.assertEquals(SHORT_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(SHORT_HALF, MySQLPacketInf.packetType);
        Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);
    }

    @Test
    public void longHalf2RestCross2Finished2ShortOnePacket() {
        ErrorPacket eofPacket = new ErrorPacket();
        eofPacket.packetId = 0;
        eofPacket.message = "";
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        eofPacket.write(buffer);
        int length = buffer.writeIndex;

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 0;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        MySQLPacketInf.proxyBuffer = buffer;
        buffer.writeIndex = HEADER_SIZE + 1;//longHalf
        Assert.assertEquals(LONG_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(LONG_HALF, MySQLPacketInf.packetType);
        Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);
        Assert.assertEquals(length, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);
        Assert.assertEquals(0xff, MySQLPacketInf.head);

        sm.crossBuffer(MySQLPacketInf);

        for (int i = buffer.writeIndex; i < length; i++) {
            buffer.writeIndex = i;
            Assert.assertEquals(LONG_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
            Assert.assertEquals(LONG_HALF, MySQLPacketInf.packetType);
            Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);
            Assert.assertEquals(length, MySQLPacketInf.pkgLength);
            Assert.assertEquals(0, MySQLPacketInf.startPos);
            Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);
            Assert.assertEquals(0xff, MySQLPacketInf.head);

            Assert.assertEquals(0,MySQLPacketInf.remainsBytes);//因为crossBuffer失败,一直没有使用剩余长度计算
        }
        buffer.writeIndex++;

        Assert.assertEquals(FULL, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(FULL, MySQLPacketInf.packetType);
        Assert.assertEquals(ERROR, sm.mysqlPacketType);
        Assert.assertEquals(length, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);
        Assert.assertEquals(0xff, MySQLPacketInf.head);

        Assert.assertEquals(0, MySQLPacketInf.remainsBytes);

        MySQLPacketInf.markRead();

        eofPacket.packetId = 2;
        eofPacket.write(buffer);
        buffer.writeIndex = length + 1;
        Assert.assertEquals(SHORT_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(SHORT_HALF, MySQLPacketInf.packetType);
        Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);
    }

    @Test
    public void fullPayloadOnePacket() {
        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        errorPacket.write(buffer);

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
        Assert.assertEquals(FULL_PAYLOAD, sm.resolveFullPayload(MySQLPacketInf));
        Assert.assertEquals(ERROR, sm.mysqlPacketType);
    }

    @Test
    public void fullPayloadMutilPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;

        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff+8));
        errorPacket.write(buffer);
        buffer.writeIndex = 0;
        buffer.writeFixInt(3,0xffffff);
        buffer.writeByte((byte) 12);
        buffer.writeIndex = 0xffffff;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        Assert.assertEquals(LONG_PAYLOAD, sm.resolveFullPayload(MySQLPacketInf));
        Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);

        MySQLPacketInf.markRead();

        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(FULL_PAYLOAD, sm.resolveFullPayload(MySQLPacketInf));
        Assert.assertEquals(ERROR, sm.mysqlPacketType);

    }

    @Test
    public void crossPayloadOnePacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;

        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff+4));
        errorPacket.write(buffer);
        buffer.writeIndex = 0;
        buffer.writeFixInt(3,0xffffff);
        buffer.writeByte((byte) 12);
        buffer.writeIndex = 0xffffff;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        Assert.assertEquals(REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);

        MySQLPacketInf.markRead();

        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        Assert.assertEquals(ERROR, sm.mysqlPacketType);

    }

    @Test
    public void crossPayloadMutilPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;

        ErrorPacket errorPacket = TestUtil.errPacket(12);
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff+4));
        errorPacket.write(buffer);
        buffer.writeIndex = 0;
        buffer.writeFixInt(3,0xffffff);
        buffer.writeByte((byte) 12);
        buffer.writeIndex = 0xffffff-1;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        Assert.assertEquals(SHORT_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);

        buffer.writeIndex = 0xffffff;
        Assert.assertEquals(REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        Assert.assertEquals(MySQLPayloadType.UNKNOWN, sm.mysqlPacketType);

        MySQLPacketInf.markRead();

        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        Assert.assertEquals(ERROR, sm.mysqlPacketType);

    }
}

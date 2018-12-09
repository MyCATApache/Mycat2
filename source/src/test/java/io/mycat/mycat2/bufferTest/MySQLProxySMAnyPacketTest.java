package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.testTool.TestUtil;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.mysql.MySQLProxyPacketResolver;
import io.mycat.mysql.PacketType;
import io.mycat.mysql.PayloadType;
import io.mycat.proxy.ProxyBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.mycat.mysql.MySQLPayloadType.UNKNOWN;


public class MySQLProxySMAnyPacketTest {

    private static int HEADER_SIZE = 4;

    /**
     * 该测试应该保证只要数据是整包,都可以得到full.payload长度从0-16
     */
    @Test
    public void fullAnyOnePacketFromZeroSize() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();

        for (int payloadLength = 0; payloadLength < 16; payloadLength++) {
            buffer.readIndex = 0;
            buffer.writeIndex = 0;
            TestUtil.anyPacket(payloadLength, 1, buffer);
            buffer.writeIndex = 4 + payloadLength;
            MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
            sm.nextPacketId = 1;
            MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
            Assert.assertEquals(PacketType.FULL, sm.resolveMySQLPacket(MySQLPacketInf));
            Assert.assertEquals(PacketType.FULL, MySQLPacketInf.packetType);
            Assert.assertEquals(HEADER_SIZE + payloadLength, MySQLPacketInf.pkgLength);
            Assert.assertEquals(0, MySQLPacketInf.startPos);
            Assert.assertEquals(true, !sm.crossPacket);
            Assert.assertEquals(MySQLPacketInf.pkgLength, MySQLPacketInf.endPos);
        }

    }

    /**
     * 该测试应该保证只要数据是整包,都可以得到full.payload长度从0xffffff-4 - 0xffffff-16
     */
    @Test
    public void fullAnyOnePacketFrom16mbSize() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        for (int payloadLength = 0xffffff - 4; payloadLength > 0xffffff - 16; payloadLength--) {
            buffer.readIndex = 0;
            buffer.writeIndex = 0;
            TestUtil.anyPacket(payloadLength, 1, buffer);
            buffer.writeIndex = 4 + payloadLength;
            MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
            sm.nextPacketId = 1;
            MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
            Assert.assertEquals(PacketType.FULL, sm.resolveMySQLPacket(MySQLPacketInf));
            Assert.assertEquals(PacketType.FULL, MySQLPacketInf.packetType);
            Assert.assertEquals(Math.min(0xffffff, payloadLength + 4), MySQLPacketInf.pkgLength);
            Assert.assertEquals(0, MySQLPacketInf.startPos);
            Assert.assertEquals(false, sm.crossPacket);
            Assert.assertEquals(MySQLPacketInf.pkgLength, MySQLPacketInf.endPos);
        }
    }

    /**
     * 该测试应该保证只要数据是整包,都可以得到full.跨报文传输
     */
    @Test
    public void fullAnyOnePacketEq16mbSize() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        int payloadLength = 0xffffff;
        buffer.readIndex = 0;
        buffer.writeIndex = 0;
        TestUtil.anyPacket(payloadLength, 1, buffer);
        buffer.writeIndex = 4 + payloadLength;
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 1;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
        Assert.assertEquals(PacketType.FULL, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.FULL, MySQLPacketInf.packetType);
        Assert.assertEquals(0xffffff, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(true, sm.crossPacket);
        Assert.assertEquals(MySQLPacketInf.pkgLength, MySQLPacketInf.endPos);
    }

    /**
     * 该测试应该保证只要数据是空报文可以从short转到full
     */
    @Test
    public void shortEmptyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
        MySQLProxyPacketResolver sm = null;
        TestUtil.anyPacket(0, 1, buffer);
        for (int i = 0; i < HEADER_SIZE; i++) {
            buffer.writeIndex = i;
            sm = new MySQLProxyPacketResolver();
            sm.nextPacketId = 1;
            Assert.assertEquals(PacketType.SHORT_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
            Assert.assertEquals(PacketType.SHORT_HALF, MySQLPacketInf.packetType);
        }
        buffer.writeIndex = HEADER_SIZE;
        Assert.assertEquals(PacketType.FULL, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.FULL, MySQLPacketInf.packetType);
    }

    /**
     * 该测试应该保证只要数据是空报文可以从short转到longHalf
     */
    @Test
    public void shortAnyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
        MySQLProxyPacketResolver sm = null;
        TestUtil.anyPacket(1, 1, buffer);
        int i = 4;
        for (; i < 5; i++) {
            buffer.writeIndex = i;
            sm = new MySQLProxyPacketResolver();
            sm.nextPacketId = 1;
            Assert.assertEquals(PacketType.SHORT_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
            Assert.assertEquals(PacketType.SHORT_HALF, MySQLPacketInf.packetType);
        }
        buffer.writeIndex = 5;
        sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 1;
        Assert.assertEquals(PacketType.FULL, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.FULL, MySQLPacketInf.packetType);
    }
    /**
     * 该测试应该保证只要数据是空报文可以从longHalf转到full
     */
    @Test
    public void longHalfAnyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(2, 1, buffer);
        int length = buffer.writeIndex = 6;
        MySQLProxyPacketResolver sm = null;
        MySQLPacketInf MySQLPacketInf = null;
        for (int i = 5; i < length; i++) {
            sm = new MySQLProxyPacketResolver();
            sm.nextPacketId = 1;
            MySQLPacketInf = new MySQLPacketInf(buffer);
            buffer.writeIndex = 5;
            Assert.assertEquals(PacketType.LONG_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
            Assert.assertEquals(PacketType.LONG_HALF, MySQLPacketInf.packetType);
            Assert.assertEquals(length, MySQLPacketInf.pkgLength);
            Assert.assertEquals(0, MySQLPacketInf.startPos);
            Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);
        }
        buffer.writeIndex = length;
        Assert.assertEquals(PacketType.FULL, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.FULL, MySQLPacketInf.packetType);
        Assert.assertEquals(length, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);
    }

    @Test
    public void longHalf2Full2ShortAnyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(2, 1, buffer);

        int length = buffer.writeIndex = 6;

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 1;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
        buffer.writeIndex = length - 1;
        Assert.assertEquals(PacketType.LONG_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.LONG_HALF, MySQLPacketInf.packetType);
        Assert.assertEquals(length, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);

        buffer.writeIndex = length;
        Assert.assertEquals(PacketType.FULL, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.FULL, MySQLPacketInf.packetType);
        Assert.assertEquals(length, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);

        MySQLPacketInf.markRead();

        buffer.writeIndex = length + 1;
        Assert.assertEquals(PacketType.SHORT_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.SHORT_HALF, MySQLPacketInf.packetType);
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
    }

    @Test
    public void longHalf2RestCross2Finished2ShortOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(6, 1, buffer);

        int length = buffer.writeIndex = 10;

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
        sm.nextPacketId = 1;
        MySQLPacketInf.proxyBuffer = buffer;
        buffer.writeIndex = HEADER_SIZE + 1;//longHalf
        Assert.assertEquals(PacketType.LONG_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.LONG_HALF, MySQLPacketInf.packetType);
        Assert.assertEquals(length, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);

        sm.crossBuffer(MySQLPacketInf);

        for (int i = buffer.writeIndex; i < length; i++) {
            buffer.writeIndex = i;
            Assert.assertEquals(PacketType.LONG_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
            Assert.assertEquals(PacketType.LONG_HALF, MySQLPacketInf.packetType);
            Assert.assertEquals(length, MySQLPacketInf.pkgLength);
            Assert.assertEquals(0, MySQLPacketInf.startPos);
            Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);

            Assert.assertEquals(0, MySQLPacketInf.remainsBytes);//因为crossBuffer失败,一直没有使用剩余长度计算
        }
        buffer.writeIndex++;

        Assert.assertEquals(PacketType.FULL, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.FULL, MySQLPacketInf.packetType);
        Assert.assertEquals(length, MySQLPacketInf.pkgLength);
        Assert.assertEquals(0, MySQLPacketInf.startPos);
        Assert.assertEquals(buffer.writeIndex, MySQLPacketInf.endPos);
        Assert.assertEquals(0, MySQLPacketInf.remainsBytes);

        MySQLPacketInf.markRead();

        buffer.writeIndex = length + 1;
        Assert.assertEquals(PacketType.SHORT_HALF, sm.resolveMySQLPacket(MySQLPacketInf));
        Assert.assertEquals(PacketType.SHORT_HALF, MySQLPacketInf.packetType);
    }

    @Test
    public void fullPayloadOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(6, 1, buffer);
        buffer.writeIndex = 10;
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 1;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);
        Assert.assertEquals(PayloadType.FULL_PAYLOAD, sm.resolveFullPayload(MySQLPacketInf));
    }

    @Test
    public void fullPayloadMutilPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 8));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        Assert.assertEquals(PayloadType.LONG_PAYLOAD, sm.resolveFullPayload(MySQLPacketInf));

        MySQLPacketInf.markRead();
        buffer.readIndex = 0;
        buffer.writeIndex = 0;
        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(PayloadType.FULL_PAYLOAD, sm.resolveFullPayload(MySQLPacketInf));


    }

    @Test
    public void crossPayloadOnePacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;

        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 8));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        Assert.assertEquals(PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));

        MySQLPacketInf.markRead();
        buffer.writeIndex = 0;
        buffer.readIndex = 0;
        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));

    }

    @Test
    public void crossPayloadMutilPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 4));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff - 1;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        Assert.assertEquals(PayloadType.SHORT_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);

        buffer.writeIndex = 0xffffff;
        Assert.assertEquals(PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));

        MySQLPacketInf.markRead();
        MySQLPacketInf.proxyBuffer.writeIndex = 0;
        MySQLPacketInf.proxyBuffer.readIndex = 0;
        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
    }
    @Test
    public void crossPayloadMutilPacket2() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 5));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff - 1;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        Assert.assertEquals(PayloadType.SHORT_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);

        buffer.writeIndex = 0xffffff;
        Assert.assertEquals(PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));

        MySQLPacketInf.markRead();
        MySQLPacketInf.proxyBuffer.readIndex = 0;
        MySQLPacketInf.proxyBuffer.writeIndex = 0;

        buffer.writeFixInt(3, 1);
        buffer.writeByte((byte) 13);
        MySQLPacketInf.proxyBuffer.writeIndex = 4;

        Assert.assertEquals(PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        MySQLPacketInf.proxyBuffer.writeIndex = 5;

        Assert.assertEquals(PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
    }

    @Test
    public void fullMetaPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.nextPacketId = 12;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 5));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff - 1;
        MySQLPacketInf MySQLPacketInf = new MySQLPacketInf(buffer);

        Assert.assertEquals(PayloadType.SHORT_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);

        buffer.writeIndex = 0xffffff;
        Assert.assertEquals(PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));

        MySQLPacketInf.markRead();
        MySQLPacketInf.proxyBuffer.readIndex = 0;
        MySQLPacketInf.proxyBuffer.writeIndex = 0;

        buffer.writeFixInt(3, 1);
        buffer.writeByte((byte) 13);
        MySQLPacketInf.proxyBuffer.writeIndex = 4;

        Assert.assertEquals(PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
        MySQLPacketInf.proxyBuffer.writeIndex = 5;

        Assert.assertEquals(PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(MySQLPacketInf));
    }
}

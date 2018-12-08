package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.testTool.TestUtil;
import io.mycat.proxy.ProxyBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.mycat.mycat2.bufferTest.MySQLRespPacketType.UNKNOWN;

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
            sm.lastPacketId = 0;
            MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
            Assert.assertEquals(HEADER_SIZE + payloadLength, packetInf.pkgLength);
            Assert.assertEquals(1, sm.lastPacketId);
            Assert.assertEquals(0, packetInf.startPos);
            Assert.assertEquals(true, !sm.crossPacket);
            Assert.assertEquals(packetInf.pkgLength, packetInf.endPos);
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
            sm.lastPacketId = 0;
            MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
            Assert.assertEquals(Math.min(0xffffff, payloadLength + 4), packetInf.pkgLength);
            Assert.assertEquals(1, sm.lastPacketId);
            Assert.assertEquals(0, packetInf.startPos);
            Assert.assertEquals(false, sm.crossPacket);
            Assert.assertEquals(packetInf.pkgLength, packetInf.endPos);
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
        sm.lastPacketId = 0;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(0xffffff, packetInf.pkgLength);
        Assert.assertEquals(1, sm.lastPacketId);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(true, sm.crossPacket);
        Assert.assertEquals(packetInf.pkgLength, packetInf.endPos);
    }

    /**
     * 该测试应该保证只要数据是空报文可以从short转到full
     */
    @Test
    public void shortEmptyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
        MySQLProxyPacketResolver sm = null;
        TestUtil.anyPacket(0, 1, buffer);
        for (int i = 0; i < HEADER_SIZE; i++) {
            buffer.writeIndex = i;
            sm = new MySQLProxyPacketResolver();
            sm.lastPacketId = 0;
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, packetInf.packetType);
        }
        buffer.writeIndex = HEADER_SIZE;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
    }

    /**
     * 该测试应该保证只要数据是空报文可以从short转到longHalf
     */
    @Test
    public void shortAnyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
        MySQLProxyPacketResolver sm = null;
        TestUtil.anyPacket(1, 1, buffer);
        int i = 4;
        for (; i < 5; i++) {
            buffer.writeIndex = i;
            sm = new MySQLProxyPacketResolver();
            sm.lastPacketId = 0;
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, packetInf.packetType);
        }
        buffer.writeIndex = 5;
        sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 0;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
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
        MySQLProxyPacketResolver.PacketInf packetInf = null;
        for (int i = 5; i < length; i++) {
            sm = new MySQLProxyPacketResolver();
            packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
            buffer.writeIndex = 5;
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, packetInf.packetType);
            Assert.assertEquals(length, packetInf.pkgLength);
            Assert.assertEquals(0, packetInf.startPos);
            Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
        }
        buffer.writeIndex = length;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
    }

    @Test
    public void longHalf2Full2ShortAnyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(2, 1, buffer);

        int length = buffer.writeIndex = 6;

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 0;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
        buffer.writeIndex = length - 1;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);

        buffer.writeIndex = length;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);

        packetInf.markRead();

        buffer.writeIndex = length + 1;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, packetInf.packetType);
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
    }

    @Test
    public void longHalf2RestCross2Finished2ShortOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(6, 1, buffer);

        int length = buffer.writeIndex = 10;

        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        packetInf.proxyBuffer = buffer;
        buffer.writeIndex = HEADER_SIZE + 1;//longHalf
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);

        sm.crossBuffer(packetInf);

        for (int i = buffer.writeIndex; i < length; i++) {
            buffer.writeIndex = i;
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyPacketResolver.PacketType.LONG_HALF, packetInf.packetType);
            Assert.assertEquals(length, packetInf.pkgLength);
            Assert.assertEquals(0, packetInf.startPos);
            Assert.assertEquals(buffer.writeIndex, packetInf.endPos);

            Assert.assertEquals(0, packetInf.remainsBytes);//因为crossBuffer失败,一直没有使用剩余长度计算
        }
        buffer.writeIndex++;

        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
        Assert.assertEquals(0, packetInf.remainsBytes);

        packetInf.markRead();

        buffer.writeIndex = length + 1;
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyPacketResolver.PacketType.SHORT_HALF, packetInf.packetType);
    }

    @Test
    public void fullPayloadOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(6, 1, buffer);
        buffer.writeIndex = 10;
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 0;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);
        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FULL_PAYLOAD, sm.resolveFullPayload(packetInf));
    }

    @Test
    public void fullPayloadMutilPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 8));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.LONG_PAYLOAD, sm.resolveFullPayload(packetInf));

        packetInf.markRead();
        buffer.readIndex = 0;
        buffer.writeIndex = 0;
        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FULL_PAYLOAD, sm.resolveFullPayload(packetInf));


    }

    @Test
    public void crossPayloadOnePacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;

        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 8));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));

        packetInf.markRead();
        buffer.writeIndex = 0;
        buffer.readIndex = 0;
        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));

    }

    @Test
    public void crossPayloadMutilPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 4));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff - 1;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.SHORT_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);

        buffer.writeIndex = 0xffffff;
        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));

        packetInf.markRead();
        packetInf.proxyBuffer.writeIndex = 0;
        packetInf.proxyBuffer.readIndex = 0;
        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
    }
    @Test
    public void crossPayloadMutilPacket2() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 5));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff - 1;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.SHORT_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);

        buffer.writeIndex = 0xffffff;
        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));

        packetInf.markRead();
        packetInf.proxyBuffer.readIndex = 0;
        packetInf.proxyBuffer.writeIndex = 0;

        buffer.writeFixInt(3, 1);
        buffer.writeByte((byte) 13);
        packetInf.proxyBuffer.writeIndex = 4;

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        packetInf.proxyBuffer.writeIndex = 5;

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
    }

    @Test
    public void fullMetaPacket() {
        MySQLProxyPacketResolver sm = new MySQLProxyPacketResolver();
        sm.lastPacketId = 11;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 5));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff - 1;
        MySQLProxyPacketResolver.PacketInf packetInf = new MySQLProxyPacketResolver.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.SHORT_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);

        buffer.writeIndex = 0xffffff;
        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));

        packetInf.markRead();
        packetInf.proxyBuffer.readIndex = 0;
        packetInf.proxyBuffer.writeIndex = 0;

        buffer.writeFixInt(3, 1);
        buffer.writeByte((byte) 13);
        packetInf.proxyBuffer.writeIndex = 4;

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.REST_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        packetInf.proxyBuffer.writeIndex = 5;

        Assert.assertEquals(MySQLProxyPacketResolver.PayloadType.FINISHED_CROSS_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
    }
}

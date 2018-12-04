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
            MySQLProxyStateMachine sm = new MySQLProxyStateMachine();
            sm.lastPacketId = 0;
            MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, packetInf.packetType);
            Assert.assertEquals(HEADER_SIZE + payloadLength, packetInf.pkgLength);
            Assert.assertEquals(1, packetInf.packetId);
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
            MySQLProxyStateMachine sm = new MySQLProxyStateMachine();
            sm.lastPacketId = 0;
            MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, packetInf.packetType);
            Assert.assertEquals(Math.min(0xffffff, payloadLength + 4), packetInf.pkgLength);
            Assert.assertEquals(1, packetInf.packetId);
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
        MySQLProxyStateMachine sm = new MySQLProxyStateMachine();
        sm.lastPacketId = 0;
        MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(0xffffff, packetInf.pkgLength);
        Assert.assertEquals(1, packetInf.packetId);
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
        MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);
        MySQLProxyStateMachine sm = null;
        TestUtil.anyPacket(0, 1, buffer);
        for (int i = 0; i < HEADER_SIZE; i++) {
            buffer.writeIndex = i;
            sm = new MySQLProxyStateMachine();
            sm.lastPacketId = 0;
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.SHORT_HALF, packetInf.packetType);
        }
        buffer.writeIndex = HEADER_SIZE;
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, packetInf.packetType);
    }

    /**
     * 该测试应该保证只要数据是空报文可以从short转到longHalf
     */
    @Test
    public void shortAnyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);
        MySQLProxyStateMachine sm = null;
        TestUtil.anyPacket(1, 1, buffer);
        int i = 0;
        for (; i < 5; i++) {
            buffer.writeIndex = i;
            sm = new MySQLProxyStateMachine();
            sm.lastPacketId = 0;
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.SHORT_HALF, packetInf.packetType);
        }
        buffer.writeIndex = 5;
        sm = new MySQLProxyStateMachine();
        sm.lastPacketId = 0;
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, packetInf.packetType);
    }
    /**
     * 该测试应该保证只要数据是空报文可以从longHalf转到full
     */
    @Test
    public void longHalfAnyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(2, 1, buffer);
        int length = buffer.writeIndex = 6;
        MySQLProxyStateMachine sm = null;
        MySQLProxyStateMachine.PacketInf packetInf = null;
        for (int i = 5; i < length; i++) {
            sm = new MySQLProxyStateMachine();
            packetInf = new MySQLProxyStateMachine.PacketInf(buffer);
            buffer.writeIndex = 5;
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.LONG_HALF, packetInf.packetType);
            Assert.assertEquals(length, packetInf.pkgLength);
            Assert.assertEquals(0, packetInf.startPos);
            Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
        }
        buffer.writeIndex = length;
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
    }

    @Test
    public void longHalf2Full2ShortAnyOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(2, 1, buffer);

        int length = buffer.writeIndex = 6;

        MySQLProxyStateMachine sm = new MySQLProxyStateMachine();
        sm.lastPacketId = 0;
        MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);
        buffer.writeIndex = length - 1;
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.LONG_HALF, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);

        buffer.writeIndex = length;
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);

        packetInf.markRead();

        buffer.writeIndex = length + 1;
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.SHORT_HALF, packetInf.packetType);
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
    }

    @Test
    public void longHalf2RestCross2Finished2ShortOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(6, 1, buffer);

        int length = buffer.writeIndex = 10;

        MySQLProxyStateMachine sm = new MySQLProxyStateMachine();
        MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);

        packetInf.proxyBuffer = buffer;
        buffer.writeIndex = HEADER_SIZE + 1;//longHalf
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.LONG_HALF, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);

        packetInf.crossBuffer();

        for (int i = buffer.writeIndex; i < length; i++) {
            buffer.writeIndex = i;
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.LONG_HALF, sm.resolveMySQLPackage(packetInf));
            Assert.assertEquals(MySQLProxyStateMachine.PacketType.LONG_HALF, packetInf.packetType);
            Assert.assertEquals(length, packetInf.pkgLength);
            Assert.assertEquals(0, packetInf.startPos);
            Assert.assertEquals(buffer.writeIndex, packetInf.endPos);

            Assert.assertEquals(0, packetInf.remainsBytes);//因为crossBuffer失败,一直没有使用剩余长度计算
        }
        buffer.writeIndex++;

        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.FULL, packetInf.packetType);
        Assert.assertEquals(length, packetInf.pkgLength);
        Assert.assertEquals(0, packetInf.startPos);
        Assert.assertEquals(buffer.writeIndex, packetInf.endPos);
        Assert.assertEquals(0, packetInf.remainsBytes);

        packetInf.markRead();

        buffer.writeIndex = length + 1;
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.SHORT_HALF, sm.resolveMySQLPackage(packetInf));
        Assert.assertEquals(MySQLProxyStateMachine.PacketType.SHORT_HALF, packetInf.packetType);
    }

    @Test
    public void fullPayloadOnePacket() {
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        TestUtil.anyPacket(6, 1, buffer);
        buffer.writeIndex = 10;
        MySQLProxyStateMachine sm = new MySQLProxyStateMachine();
        sm.lastPacketId = 0;
        MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);
        Assert.assertEquals(MySQLProxyStateMachine.PayloadType.FULL_PAYLOAD, sm.resolveFullPayload(packetInf));
    }

    @Test
    public void fullPayloadMutilPacket() {
        MySQLProxyStateMachine sm = new MySQLProxyStateMachine();
        sm.lastPacketId = 11;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 8));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff;
        MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyStateMachine.PayloadType.TYPE_PAYLOAD, sm.resolveFullPayload(packetInf));

        packetInf.markRead();

        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(MySQLProxyStateMachine.PayloadType.FULL_PAYLOAD, sm.resolveFullPayload(packetInf));


    }

    @Test
    public void crossPayloadOnePacket() {
        MySQLProxyStateMachine sm = new MySQLProxyStateMachine();
        sm.lastPacketId = 11;

        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 8));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff;
        MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyStateMachine.PayloadType.HALF_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));

        packetInf.markRead();

        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(MySQLProxyStateMachine.PayloadType.FULL_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));

    }

    @Test
    public void crossPayloadMutilPacket() {
        MySQLProxyStateMachine sm = new MySQLProxyStateMachine();
        sm.lastPacketId = 11;
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(0xffffff + 4));
        TestUtil.anyPacket(0xffffff, 12, buffer);
        buffer.writeIndex = 0xffffff - 1;
        MySQLProxyStateMachine.PacketInf packetInf = new MySQLProxyStateMachine.PacketInf(buffer);

        Assert.assertEquals(MySQLProxyStateMachine.PayloadType.HALF_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));
        Assert.assertEquals(UNKNOWN, sm.mysqlPacketType);
        Assert.assertEquals(false, sm.willBeFinished);

        buffer.writeIndex = 0xffffff;
        Assert.assertEquals(MySQLProxyStateMachine.PayloadType.HALF_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));

        packetInf.markRead();

        buffer.writeFixInt(3, 0);
        buffer.writeByte((byte) 13);

        Assert.assertEquals(MySQLProxyStateMachine.PayloadType.FULL_PAYLOAD, sm.resolveCrossBufferFullPayload(packetInf));


    }
}

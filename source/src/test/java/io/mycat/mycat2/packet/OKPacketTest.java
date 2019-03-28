package io.mycat.mycat2.packet;

import io.mycat.mycat2.testTool.TestUtil;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.mycat.mycat2.testTool.TestUtil.ofBuffer;


/**
 * Created by linxiaofang on 2018/11/12.
 */
public class OKPacketTest {
    @Test
    public void testOKPacket() {

        OKPacket okPacket = new OKPacket(Capabilities.CLIENT_PROTOCOL_41);
        okPacket.read(ofBuffer(okPkt));
        Assert.assertEquals(1, okPacket.affectedRows);
        Assert.assertEquals(1, okPacket.lastInsertId);
        Assert.assertEquals(0, okPacket.warningCount);

        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(okPkt.length));
        okPacket.write(buffer);
        byte[] array = buffer.getBuffer().array();
        final String hexs = StringUtil.dumpAsHex(array);
        System.out.println(hexs);
        int[] ints = Arrays.copyOf(okPkt, okPkt.length);
        Assert.assertArrayEquals(TestUtil.of(ints),array);
        Assert.assertEquals(okPacket.calcPayloadSize()+4, okPkt.length);
    }

//    @Test
//    public void testSetAutoCommitOffPacket() {
//        OKPacket okPacket = new OKPacket(Capabilities.CLIENT_PROTOCOL_41);
//        okPacket.read(ofBuffer(setAutoCommitOffPkt));
//        Assert.assertEquals(0, okPacket.affectedRows);
//        Assert.assertEquals(0, okPacket.lastInsertId);
//        Assert.assertEquals(0, okPacket.warningCount);
//
//        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(setAutoCommitOffPkt.length));
//        okPacket.write(buffer);
//        byte[] array = buffer.getBuffer().array();
//        final String hexs = StringUtil.dumpAsHex(array);
//        System.out.println(hexs);
//        int[] ints = Arrays.copyOf(setAutoCommitOffPkt, setAutoCommitOffPkt.length);
//        Assert.assertArrayEquals(of(ints),array);
//        Assert.assertEquals(okPacket.calcPayloadSize()+4, setAutoCommitOffPkt.length);
//    }
//
//    @Test
//    public void testUseDbPacket() {
//        OKPacket okPacket = new OKPacket(Capabilities.CLIENT_PROTOCOL_41);
//        okPacket.read(ofBuffer(useDbPkt));
//        Assert.assertEquals(0, okPacket.affectedRows);
//        Assert.assertEquals(0, okPacket.lastInsertId);
//        Assert.assertEquals(0, okPacket.warningCount);
//
//        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(useDbPkt.length));
//        okPacket.write(buffer);
//        byte[] array = buffer.getBuffer().array();
//        final String hexs = StringUtil.dumpAsHex(array);
//        System.out.println(hexs);
//        int[] ints = Arrays.copyOf(useDbPkt, useDbPkt.length);
//        Assert.assertArrayEquals(of(ints),array);
//        Assert.assertEquals(okPacket.calcPayloadSize()+4, useDbPkt.length);
//    }
//
//    @Test
//    public void testSetSessionPacket() {
//        OKPacket okPacket = new OKPacket(Capabilities.CLIENT_PROTOCOL_41);
//        okPacket.read(ofBuffer(setSessionPkt));
//        Assert.assertEquals(0, okPacket.affectedRows);
//        Assert.assertEquals(0, okPacket.lastInsertId);
//        Assert.assertEquals(0, okPacket.warningCount);
//
//        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(setSessionPkt.length));
//        okPacket.write(buffer);
//        byte[] array = buffer.getBuffer().array();
//        final String hexs = StringUtil.dumpAsHex(array);
//        System.out.println(hexs);
//        int[] ints = Arrays.copyOf(setSessionPkt, setSessionPkt.length);
//        Assert.assertArrayEquals(of(ints),array);
//        Assert.assertEquals(okPacket.calcPayloadSize()+4, setSessionPkt.length);
//    }

    @Test
    public void testEOFPacket() {
        OKPacket okPacket = new OKPacket(Capabilities.CLIENT_PROTOCOL_41);
        okPacket.read(ofBuffer(eofPkt));

        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(eofPkt.length));
        okPacket.write(buffer);
        byte[] array = buffer.getBuffer().array();
        final String hexs = StringUtil.dumpAsHex(array);
        System.out.println(hexs);
        int[] ints = Arrays.copyOf(eofPkt, eofPkt.length);
        Assert.assertArrayEquals(TestUtil.of(ints),array);
        Assert.assertEquals(okPacket.calcPayloadSize()+4, eofPkt.length);
    }

    /*
    * Example: for a query like insert into TEST VALUES(1);
    */
    static int[] okPkt = {
            0x07, 0x00, 0x00, 0x01, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00
    };

//    /*
//     * Example: for a query like SET autocommit = OFF :
//     */
//    static int[] setAutoCommitOffPkt = {
//            0x07, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
//    };
//
//    /*
//     * Example: for a query like Use test:
//     */
//    static int[] useDbPkt = {
//            0x07, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
//
//    };
//
//    /*
//     * Example: for a query like SET SESSION session_track_state_change = 1:
//     */
//    static int[] setSessionPkt = {
//            0x07, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
//    };

    static int[] eofPkt = {
            0x05,0x00,0x00,0x05,0xfe,0x00,0x00,0x02,0x00
    };
}

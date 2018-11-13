package io.mycat.mycat2.packet;

import io.mycat.mysql.packet.PreparedOKPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.mycat.mycat2.TestUtil.of;
import static io.mycat.mycat2.TestUtil.ofBuffer;

/**
 * Created by linxiaofang on 2018/11/12.
 */
public class PreparedOKPacketTest {
    @Test
    public void testReadPreparedOKPacket() {
        PreparedOKPacket preparedOKPacket = new PreparedOKPacket();
        preparedOKPacket.readPayload(ofBuffer(pkt17));
        Assert.assertEquals(0x00, preparedOKPacket.status);
        Assert.assertEquals(1, preparedOKPacket.columnsNumber);
        Assert.assertEquals(2, preparedOKPacket.parametersNumber);
        Assert.assertEquals(0x00, preparedOKPacket.filler);
        Assert.assertEquals(0x00, preparedOKPacket.warningCount);
    }

    @Test
    public void testWritePreparedOKPacket() {
        PreparedOKPacket fact = new PreparedOKPacket();
        fact.readPayload(ofBuffer(pkt17));

        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(pkt17.length));
        fact.write(buffer);
        byte[] array = buffer.getBuffer().array();
        final String hexs = StringUtil.dumpAsHex(array);
        System.out.println(hexs);

        int[] ints = Arrays.copyOf(pkt17, pkt17.length);
        Assert.assertArrayEquals(of(ints),array);
        System.out.println(fact.calcPacketSize());
        System.out.println(pkt17.length);
        Assert.assertEquals(fact.calcPacketSize()+4, pkt17.length);
    }

    /*
     * Example: for a prepared query like SELECT CONCAT(?, ?) AS col1:
     */
    static int[] pkt17 = {
            0x0c,0x00,0x00,0x01,0x00,0x01,0x00,0x00,0x00,0x01,0x00,0x02,0x00,0x00,0x00,0x00,        /* ................ */
            0x17,0x00,0x00,0x02,0x03,0x64,0x65,0x66,0x00,0x00,0x00,0x01,0x3f,0x00,0x0c,0x3f,        /* .....def....?..? */
            0x00,0x00,0x00,0x00,0x00,0xfd,0x80,0x00,0x00,0x00,0x00,0x17,0x00,0x00,0x03,0x03,        /* ................ */
            0x64,0x65,0x66,0x00,0x00,0x00,0x01,0x3f,0x00,0x0c,0x3f,0x00,0x00,0x00,0x00,0x00,        /* def....?..?..... */
            0xfd,0x80,0x00,0x00,0x00,0x00,0x05,0x00,0x00,0x04,0xfe,0x00,0x00,0x02,0x00,0x1a,        /* ................ */
            0x00,0x00,0x05,0x03,0x64,0x65,0x66,0x00,0x00,0x00,0x04,0x63,0x6f,0x6c,0x31,0x00,        /* ....def....col1. */
            0x0c,0x3f,0x00,0x00,0x00,0x00,0x00,0xfd,0x80,0x00,0x1f,0x00,0x00,0x05,0x00,0x00,        /* .?.............. */
            0x06,0xfe,0x00,0x00,0x02,0x00
    };
}



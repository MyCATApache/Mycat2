package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.beans.conf.ProxyBean;
import io.mycat.mycat2.beans.conf.ProxyConfig;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.DirectByteBufferPool;
import io.mycat.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.mycat.mycat2.TestUtil.of;
import static io.mycat.mycat2.TestUtil.ofBuffer;
import static io.mycat.mysql.packet.CurrPacketType.*;

public class MySQLPackageInfTest {

    public static AbstractMySQLSession mock(ProxyBuffer proxyBuffer) {
        AbstractMySQLSession sqlSession = new AbstractMySQLSession() {
            @Override
            protected void doTakeReadOwner() {
            }
        };
        sqlSession.proxyBuffer = proxyBuffer;
        sqlSession.curMSQLPackgInf = new MySQLPackageInf();
        return sqlSession;
    }

    /*
     * FullPacket读写测试
     */
    @Test
    public void testFullPacket() {
        //第一次写入读出
        ResultSetHeaderPacket headerPacket = new ResultSetHeaderPacket();
        headerPacket.fieldCount = 1;//不可能为0
        Assert.assertTrue(headerPacket.fieldCount > 0);
        headerPacket.extra = 0;
        headerPacket.packetId = 0;
        ByteBuffer allocate = ByteBuffer.allocate(128);
        ProxyBuffer proxyBuffer = new ProxyBuffer(allocate);
        headerPacket.write(proxyBuffer);
        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage(true);
        Assert.assertEquals(currPacketType, Full);
        Assert.assertTrue(mySQLSession.curMSQLPackgInf.isFieldsCount());
        //在写入两个整包,并读出
        //testFullFullPacket();
    }

    @Test
    public void testPrepareStatementResponse() {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        ProxyBuffer proxyBuffer = new ProxyBuffer(buffer);
        System.out.println(peer1_23.length);
        for (int i : peer1_23) {
            proxyBuffer.writeByte((byte) i);
        }

        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        while (mySQLSession.isResolveMySQLPackageFinished()) {
            CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage(true);
            Assert.assertEquals(currPacketType, Full);
            System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
        }

    }

    //LongHalf
    //命令show databases
    //完整的包
    //0x0d, 0x00, 0x00, 0x00, 0x03, 0x73, 0x68, 0x6f, 0x77, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x3b
    //LongHalf没有接受到完整的报文,
    //测试模拟没有接受到最后一个报文0x3b
    @Test
    public void testLongHalfPacket() {

        int[] peer = new int[]{
                0x0d, 0x00, 0x00, 0x00, 0x03, 0x73, 0x68,
                0x6f, 0x77, 0x20, 0x74, 0x61, 0x62, 0x6c,
                0x65, 0x73
        };
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        ProxyBuffer proxyBuffer = new ProxyBuffer(buffer);
        for (int i : peer) {
            proxyBuffer.writeByte((byte) i);
        }

        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        mySQLSession.bufPool = new DirectByteBufferPool((1024 * 1024 * 4), (short) (1024 * 4 * 2), (short) 64);
        MycatConfig mycatConfig = new MycatConfig();
        ProxyConfig config = new ProxyConfig();
        ProxyBean bean = new ProxyBean();
        config.setProxy(bean);
        bean.setMax_allowed_packet(1024);
        mycatConfig.putConfig(ConfigEnum.PROXY, config, 0);
        ProxyRuntime.INSTANCE.setConfig(mycatConfig);

        while (mySQLSession.isResolveMySQLPackageFinished()) {
            CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage(true);
            if (currPacketType == Full) {
                System.out.println("-----FullPacket---");
                System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
            }
            if (currPacketType == CurrPacketType.LongHalfPacket) {
                System.out.println("-----LongHalfPacket---");
                System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));

                //再次写入剩下的short包 查看proxyBuffer是否异常
                int[] peer1 = new int[]{
                        0x3b
                };
                //写入测试
                for (int i : peer1) {
                    proxyBuffer.writeByte((byte) i);
                }
            }
        }
    }

    //ShortHalf
    //show databases ok报文
    //完整的包
    //0x0d, 0x00, 0x00, 0x00, 0x03, 0x73, 0x68, 0x6f, 0x77, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x3b
    //shortHalf不能判断类型的报文,
    //测试模拟接受到报文0x6c, 0x65, 0x73, 0x3b
    @Test
    public void testOkShortHalfPacket() {
        int[] peer = new int[]{
                0x0d, 0x00, 0x00, 0x00
        };
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        ProxyBuffer proxyBuffer = new ProxyBuffer(buffer);
        System.out.println(peer.length);
        for (int i : peer) {
            proxyBuffer.writeByte((byte) i);
        }
        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage(true);
        Assert.assertSame(CurrPacketType.ShortHalfPacket, currPacketType);
        //再次写入剩下的包 查看proxyBuffer是否异常
        int[] peer1 = new int[]{
                0x03, 0x73, 0x68, 0x6f, 0x77, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x3b
        };
        //写入测试
        for (int i : peer1) {
            proxyBuffer.writeByte((byte) i);
        }
        Assert.assertSame(Full, mySQLSession.resolveMySQLPackage(true));
    }

    /**
     * packet length 3
     * packet number 1
     * number of felds 1000
     */
    @Test
    public void testResultSetHeadFullPacketExmaple() {
        ProxyBuffer buffer = ofBuffer(0x03, 0x00, 0x00, 0x01, 0xfc, 0xe8, 0x03, 0x3b, 0x00);
        AbstractMySQLSession mySQLSession = mock(buffer);
        CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage(true);
        Assert.assertSame(currPacketType, Full);
    }

    @Test
    public void testResultSetHeadFullPacket() {
        ProxyBuffer buffer = ofBuffer(0x01, 0x00, 0x00, 0x01, 0xfc, 0x01);
        AbstractMySQLSession mySQLSession = mock(buffer);
        CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage(true);
        Assert.assertSame(currPacketType, Full);
        Assert.assertTrue(mySQLSession.curMSQLPackgInf.isFieldsCount());
        Assert.assertTrue(!mySQLSession.curMSQLPackgInf.isOkPacket());
    }

    //ShortHalf
    //测试传输了result Set首包,大于251时
    @Test
    public void testResultSetShortHalfPacket() {
        ProxyBuffer buffer = ofBuffer(0x01, 0x00, 0x00, 0x01, 0xfc);
        AbstractMySQLSession mySQLSession = mock(buffer);
        Assert.assertSame(Full, mySQLSession.resolveMySQLPackage(true));
        Assert.assertTrue(mySQLSession.curMSQLPackgInf.isFieldsCount());
        Assert.assertTrue(!mySQLSession.curMSQLPackgInf.isOkPacket());
    }

    /**
     * FullLongHalf测试
     * 完整包
     * 01 00 00 01 05
     * 2b 00 00 02 03 64 65 66 03 64 62 32 07 6d 65 73
     * 73 61 67 65 07 6d 65 73 73 61 67 65 02 69 64 02
     * 69 64 0c 3f 00 0b 00 00 00 03 0b 42 00 00 00
     */
    @Test
    public void testFullLongHalfPacket() {
        int[] peer = new int[]{
                0x01, 0x00, 0x00, 0x01, 0x05, //result set首包,一个full包
                0x2b, 0x00, 0x00, 0x02, 0x03, //longhalf包
                0x64, 0x65, 0x66
        };
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        ProxyBuffer proxyBuffer = new ProxyBuffer(buffer);
        //写入测试
        for (int i : peer) {
            proxyBuffer.writeByte((byte) i);
        }
        //读取测试
        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        mySQLSession.bufPool = new DirectByteBufferPool((1024 * 1024 * 4), (short) (1024 * 4 * 2), (short) 64);
        while (mySQLSession.isResolveMySQLPackageFinished()) {
            CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage(true);
            if (currPacketType == Full) {
                System.out.println("-----FullPacket---");
                System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
            }
            if (currPacketType == CurrPacketType.LongHalfPacket) {
                System.out.println("-----LongHalfPacket---");
                System.out.println(StringUtil.dumpMySQLPackageInfAsHex(mySQLSession));

                //再次写入剩下的short包 查看proxyBuffer是否异常
                int[] peer1 = new int[]{
                        0x03, 0x64, 0x62, 0x32, 0x07, 0x6d, 0x65, 0x73,
                        0x73, 0x61, 0x67, 0x65, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x02, 0x69, 0x64, 0x02,
                        0x69, 0x64, 0x0c, 0x3f, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x03, 0x0b, 0x42, 0x00, 0x00, 0x00
                };
                //写入测试
                for (int i : peer1) {
                    proxyBuffer.writeByte((byte) i);
                }
            }
        }
    }

    /**
     * FullShortHalf测试  有错
     * 完整包
     * 07 00 00 11 01 39 00 fb 01 39 fb
     * 05 00 00 12 fe 00 00 22 00
     */
    @Test
    public void testFullShortHalfPacket() {
        int[] peer = new int[]{
                0x07, 0x00, 0x00, 0x11, 0x01, 0x39, 0x00, 0xfb, 0x01, 0x39, 0xfb, //full
                0x00, 0x22, 0x00, //short
        };
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        ProxyBuffer proxyBuffer = new ProxyBuffer(buffer);
        for (int i : peer) {
            proxyBuffer.writeByte((byte) i);
        }

        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        while (mySQLSession.isResolveMySQLPackageFinished()) {
            CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage(true);
            if (currPacketType == Full) {
                System.out.println("-----FullPacket---");
                System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
            }
            if (currPacketType == CurrPacketType.ShortHalfPacket) {
                System.out.println("-----ShortPacket---");
                System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(),
                        mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));

                //再次写入剩下的short包 查看proxyBuffer是否异常
                int[] peer1 = new int[]{
                        0x05, 0x00, 0x00, 0x12, 0xfe
                };
                //写入测试
                for (int i : peer1) {
                    proxyBuffer.writeByte((byte) i);
                }
                break;
            }
        }
    }

    /**
     * FullFull测试
     * 完整包
     * 01 00 00 01 05
     * 2b 00 00 02 03 64 65 66 03 64 62 32 07 6d 65 73
     * 73 61 67 65 07 6d 65 73 73 61 67 65 02 69 64 02
     * 69 64 0c 3f 00 0b 00 00 00 03 0b 42 00 00 00
     */
    @Test
    public void testFullFullPacket() {
        int[] peer = new int[]{
                0x07, 0x00, 0x00, 0x11, 0x01, 0x39, 0x00, 0xfb, 0x01, 0x39, 0xfb, //full
                0x05, 0x00, 0x00, 0x12, 0xfe, 0x00, 0x00, 0x22, 0x00
        };
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        ProxyBuffer proxyBuffer = new ProxyBuffer(buffer);
        for (int i : peer) {
            proxyBuffer.writeByte((byte) i);
        }

        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        while (mySQLSession.isResolveMySQLPackageFinished()) {
            CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage(true);
            Assert.assertEquals(currPacketType, Full);
            System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
        }
        //再次写入一个整包,并读出
        testFullPacket();
    }

    /**
     * cjw
     * 294712221@qq.com
     * 因为buffer容量不足以存在报文,进行crossBuffer解析
     */
    @Test
    public void testCrossBufferPacket() {
        int[] ok = new int[]{0x0d, 0x00, 0x00, 0x00, 0x03,
                0x73, 0x68, 0x6f, 0x77, 0x20,
                0x74, 0x61, 0x62, 0x6c, 0x65,
                0x73, 0x3b};
        int length = ok.length;
        ByteBuffer allocate = ByteBuffer.allocate(5);
        ProxyBuffer buffer = new ProxyBuffer(allocate);
        AbstractMySQLSession sqlSession = mock(buffer);

        checkWriteAndChange2(sqlSession, ok[0], CurrPacketType.ShortHalfPacket, false);
        checkWriteAndChange2(sqlSession, ok[1], CurrPacketType.ShortHalfPacket, false);
        checkWriteAndChange2(sqlSession, ok[2], CurrPacketType.ShortHalfPacket, false);
        checkWriteAndChange2(sqlSession, ok[3], CurrPacketType.ShortHalfPacket, false);
        checkWriteAndChange2(sqlSession, ok[4], CurrPacketType.LongHalfPacket, true);

        //进入crossBuffer状态
        someoneTakeAway(sqlSession);

        checkWriteAndChange2(sqlSession, ok[5], CurrPacketType.RestCrossBufferPacket, true);
        checkWriteAndChange2(sqlSession, ok[6], CurrPacketType.RestCrossBufferPacket, true);

        //可以任意取走数据
        someoneTakeAway(sqlSession);

        checkWriteAndChange2(sqlSession, ok[7], CurrPacketType.RestCrossBufferPacket, true);
        checkWriteAndChange2(sqlSession, ok[8], CurrPacketType.RestCrossBufferPacket, true);
        checkWriteAndChange2(sqlSession, ok[9], CurrPacketType.RestCrossBufferPacket, true);

        for (int i = 10; i < 16; i++) {
            someoneTakeAway(sqlSession);
            checkWriteAndChange2(sqlSession, ok[i], CurrPacketType.RestCrossBufferPacket, true);
        }
        checkWriteAndChange2(sqlSession, ok[16], FinishedCrossBufferPacket, true);
        //接受新的报文
        checkWriteAndChange2(sqlSession, 0x0d, CurrPacketType.ShortHalfPacket, false);

        Assert.assertEquals(allocate.capacity(), 5);
    }

    /**
     * cjw
     * 294712221@qq.com
     * 根据不同的场景,对LongHalf报文尽可能地交换数据,进行crossBuffer解析
     */
    @Test
    public void testCrossBufferFullLongHalfShortHalfPacket() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        ProxyBuffer buffer = new ProxyBuffer(byteBuffer);
        for (byte b : of(0x07, 0x00, 0x00, 0x11, 0x01, 0x39, 0x00, 0xfb, 0x01, 0x39, 0xfb)) {
            buffer.writeByte(b);
        }
        AbstractMySQLSession sqlSession = mock(buffer);
        MySQLPackageInf curMSQLPackgInf = sqlSession.curMSQLPackgInf;
        CurrPacketType currPacketType = sqlSession.resolveMySQLPackage(true);//自动markread
        Assert.assertEquals(Full, currPacketType);
        byte[] ok = of(0x0d, 0x00, 0x00, 0x00, 0x03,
                0x73, 0x68, 0x6f, 0x77, 0x20,
                0x74, 0x61, 0x62, 0x6c, 0x65,
                0x73, 0x3b);
        for (int i = 0; i < 5; i++) {
            buffer.writeByte(ok[i]);
        }
        Assert.assertEquals(LongHalfPacket, sqlSession.resolveMySQLPackage(true));
        sqlSession.forceCrossBuffer();
        Assert.assertTrue(curMSQLPackgInf.crossBuffer);
        someoneTakeAway(sqlSession);
        for (int i = 5; i < ok.length - 1; i++) {
            buffer.writeByte(ok[i]);
            Assert.assertEquals(RestCrossBufferPacket, sqlSession.resolveCrossBufferMySQLPackage());
        }
        buffer.writeByte(ok[ok.length - 1]);
        Assert.assertEquals(FinishedCrossBufferPacket, sqlSession.resolveCrossBufferMySQLPackage());
        checkWriteAndChange2(sqlSession, 0x0d, CurrPacketType.ShortHalfPacket, false);

        Assert.assertEquals(byteBuffer.capacity(), 16);
    }

//    @Test
//    public void testCrossBufferRestLongHalfFullToFullPacket() {
//        ByteBuffer byteBuffer = ByteBuffer.allocate(30);
//        ProxyBuffer buffer = new ProxyBuffer(byteBuffer);
//        //RestLongHalf包  0x07, 0x00, 0x00, 0x11, 0x01,
//        byte[] ok1 = of(0x0d, 0x00, 0x00, 0x00, 0x03, 0x07, 0x00, 0x00, 0x11, 0x01, 0x39, 0x00, 0xfb, 0x01, 0x39, 0xfb);
//        for (int i = 5; i < ok1.length; i++) {
//            buffer.writeByte(ok1[i]);
//        }
//        AbstractMySQLSession sqlSession = mock(buffer);
//        sqlSession.bufPool = new DirectByteBufferPool((1024 * 1024 * 4), (short) (1024 * 4 * 2), (short) 64);
//
//        MySQLPackageInf curMSQLPackgInf = sqlSession.curMSQLPackgInf;
//        CurrPacketType currPacketType = sqlSession.resolveMySQLPackage(true);//自动markread
////        Assert.assertEquals(ShortHalfPacket,currPacketType);
//
//        //full包
//        for (byte b : of(0x07, 0x00, 0x00, 0x11, 0x01, 0x39, 0x00, 0xfb, 0x01, 0x39, 0xfb)) {
//            buffer.writeByte(b);
//        }
//        sqlSession.resolveMySQLPackage(true);
////        Assert.assertEquals(Full,sqlSession.resolveMySQLPackage(true));
//
//        //透传
//        sqlSession.forceCrossBuffer();
//        Assert.assertTrue(curMSQLPackgInf.crossBuffer);
//        someoneTakeAway(sqlSession);
//
//        buffer.writeByte((byte) 0x11);
//
//        Assert.assertEquals(Full, sqlSession.resolveCrossBufferMySQLPackage());
//    }

    private void someoneTakeAway(AbstractMySQLSession sqlSession) {
        sqlSession.proxyBuffer.reset();
        sqlSession.curMSQLPackgInf.startPos = 0;
        sqlSession.curMSQLPackgInf.endPos = 0;
    }

    private void checkWriteAndChange2(AbstractMySQLSession sqlSession, int b, CurrPacketType type, boolean crossBuffer) {
        sqlSession.proxyBuffer.writeByte((byte) b);
        Assert.assertEquals(type, sqlSession.resolveCrossBufferMySQLPackage());
        Assert.assertEquals(crossBuffer, sqlSession.curMSQLPackgInf.crossBuffer);
    }


    int[] peer1_23 = new int[]{ /* Packet 175628 */
            0x0d, 0x00, 0x00, 0x00, 0x03, 0x73, 0x68, 0x6f, 0x77, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73,
            0x3b
    };

}

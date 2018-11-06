package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

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

    @Test
    public void testFullPacket() {
        ResultSetHeaderPacket headerPacket = new ResultSetHeaderPacket();
        headerPacket.fieldCount = 0;
        headerPacket.extra = 0;
        headerPacket.packetId = 0;
        ByteBuffer allocate = ByteBuffer.allocate(128);
        ProxyBuffer proxyBuffer = new ProxyBuffer(allocate);
        headerPacket.write(proxyBuffer);
        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        AbstractMySQLSession.CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage();
        Assert.assertEquals(currPacketType, AbstractMySQLSession.CurrPacketType.Full);
        Assert.assertTrue(mySQLSession.curMSQLPackgInf.isFiledCount());
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
            AbstractMySQLSession.CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage();
            Assert.assertEquals(currPacketType, AbstractMySQLSession.CurrPacketType.Full);
            System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
        }

    }
    
    //1514字节包，未结束
    @Test
    public void testPrepareStatementResponseLongHalfPacket() {
    	String str = "010000010727000002036465660364623205745f6c6f6705745f6c6f670269640269640c3f000b00000003034200000033000003036465660364623205745f6c6f6705745f6c6f6708757365726e616d6508757365726e616d650c210096000000fd09500000002f000004036465660364623205745f6c6f6705745f6c6f67066d6f64756c65066d6f64756c650c21002c010000fd01100000002f000005036465660364623205745f6c6f6705745f6c6f6706706172616d7306706172616d730c2100fdff0200fc10000000002f000006036465660364623205745f6c6f6705745f6c6f670672656d61726b0672656d61726b0c2100fdff0200fc10000000002b000007036465660364623205745f6c6f6705745f6c6f6704666c616704666c61670c3f000100000001011000000037000008036465660364623205745f6c6f6705745f6c6f670a63726561746554696d650a63726561746554696d650c3f00130000000c895000000005000009fe000022003b00000a01310561646d696e054c4f47494efb15e794a8e688b7e5908de5af86e7a081e799bbe99986013113323031382d30382d30352031333a30353a34333b00000b01320561646d696e054c4f47494efb15e794a8e688b7e5908de5af86e7a081e799bbe99986013113323031382d30382d31302031303a34343a34393b00000c01330561646d696e054c4f47494efb15e794a8e688b7e5908de5af86e7a081e799bbe99986013113323031382d30382d31302031353a30343a31393b00000d01340561646d696e054c4f47494efb15e794a8e688b7e5908de5af86e7a081e799bbe99986013113323031382d30382d31302031373a30353a34316e00000e01350561646d696e0f5550444154455f50415353574f52442f7b226f6c6450617373776f7264223a22313233343536222c226e657750617373776f7264223a22313131313131227d0fe697a7e5af86e7a081e99499e8afaf013013323031382d30382d31302031373a31343a31326e00000f01360561646d696e0f5550444154455f50415353574f52442f7b226f6c6450617373776f7264223a22313233343536222c226e657750617373776f7264223a22313131313131227d0fe697a7e5af86e7a081e99499e8afaf013013323031382d30382d31302031373a31363a33326e00001001370561646d696e0f5550444154455f50415353574f52442f7b226f6c6450617373776f7264223a22313233343536222c226e657750617373776f7264223a22313131313131227d0fe697a7e5af86e7a081e99499e8afaf013013323031382d30382d31302031373a31363a35386400001101380561646d696e0f5550444154455f50415353574f5244257b226f6c6450617373776f7264223a2234222c226e657750617373776f7264223a2234227d0fe697a7e5af86e7a081e99499e8afaf013013323031382d30382d31302031373a31373a33363b00001201390561646d696e054c4f47494efb15e794a8e688b7e5908de5af86e7a081e799bbe99986013113323031382d30382d31332031383a30383a35353c0000130231300561646d696e054c4f47494efb15e794a8e688b7e5908de5af86e7a081e799bbe99986013113323031382d30382d31342031303a33323a32336f0000140231310561646d696e0f5550444154455f50415353574f52442f7b226f6c6450617373776f7264223a22313233343536222c226e657750617373776f7264223a22313131313131227d0fe697a7e5af86e7a081e99499e8afaf013013323031382d30382d31342031303a35303a33336f0000150231320561646d696e0f5550444154455f50415353574f52442f7b226f6c6450617373776f7264223a22313233343536222c226e657750617373776f7264223a22313131313131227d0fe697a7e5af86e7a081e99499e8afaf013013323031382d30382d31342031303a35313a34336f0000160231330561646d696e0f5550444154455f50415353574f52442f7b226f6c";
    	
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        ProxyBuffer proxyBuffer = new ProxyBuffer(buffer);

        int[] peer1_24 = new int[str.length()/2];
        for(int i=0; i < str.length(); i++){
        	  String c = str.substring(i, i+2);
        	  int a = Integer.valueOf(c,16);
        	  peer1_24[i/2] = a;
    		  i++;
    	}
        for (int i : peer1_24) {
            proxyBuffer.writeByte((byte) i);
        }

        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        while (mySQLSession.isResolveMySQLPackageFinished()) {
            AbstractMySQLSession.CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage();
            Assert.assertEquals(currPacketType, AbstractMySQLSession.CurrPacketType.Full);
            System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
        }

    }

    
  int[] peer1_23 = new int[]{ /* Packet 175628 */
		  0x0d, 0x00, 0x00, 0x00, 0x03, 0x73, 0x68, 0x6f, 0x77, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73,
	      0x3b
  };
      
    
   //use db2
//  int[] peer1_23 = new int[]{ /* Packet 175628 */
//		  0x04, 0x00, 0x00, 0x00, 0x02, 0x64, 0x62, 0x32
//  };
    
    //ok报文
//    int[] peer1_23 = new int[]{ /* Packet 175628 */
//    		0x07, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00
//    };
  
    
    //S H O W . D A T A B A S E S
//    int[] peer1_23 = new int[]{ /* Packet 175628 */
//    		0x0f, 0x00, 0x00, 0x00, 0x03, 0x53, 0x48, 0x4f, 0x57, 0x20, 0x44, 0x41, 0x54, 0x41, 0x42, 0x41,
//    		0x53, 0x45, 0x53
//    };
      
    //多个整包
//    int[] peer1_23 = new int[]{ /* Packet 175628 */
//            0x0c, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00,
//            0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00,
//            0x17, 0x00, 0x00, 0x02, 0x03, 0x64, 0x65, 0x66,
//            0x00, 0x00, 0x00, 0x01, 0x3f, 0x00, 0x0c, 0x3f,
//            0x00, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x80, 0x00,
//            0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x03, 0x03,
//            0x64, 0x65, 0x66, 0x00, 0x00, 0x00, 0x01, 0x3f,
//            0x00, 0x0c, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00,
//            0xfd, 0x80, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00,
//            0x00, 0x04, 0x03, 0x64, 0x65, 0x66, 0x00, 0x00,
//            0x00, 0x01, 0x3f, 0x00, 0x0c, 0x3f, 0x00, 0x00,
//            0x00, 0x00, 0x00, 0xfd, 0x80, 0x00, 0x00, 0x00,
//            0x00, 0x17, 0x00, 0x00, 0x05, 0x03, 0x64, 0x65,
//            0x66, 0x00, 0x00, 0x00, 0x01, 0x3f, 0x00, 0x0c,
//            0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x80,
//            0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x06,
//            0x03, 0x64, 0x65, 0x66, 0x00, 0x00, 0x00, 0x01,
//            0x3f, 0x00, 0x0c, 0x3f, 0x00, 0x00, 0x00, 0x00,
//            0x00, 0xfd, 0x80, 0x00, 0x00, 0x00, 0x00, 0x17,
//            0x00, 0x00, 0x07, 0x03, 0x64, 0x65, 0x66, 0x00,
//            0x00, 0x00, 0x01, 0x3f, 0x00, 0x0c, 0x3f, 0x00,
//            0x00, 0x00, 0x00, 0x00, 0xfd, 0x80, 0x00, 0x00,
//            0x00, 0x00, 0x17, 0x00, 0x00, 0x08, 0x03, 0x64,
//            0x65, 0x66, 0x00, 0x00, 0x00, 0x01, 0x3f, 0x00,
//            0x0c, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfd,
//            0x80, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00,
//            0x09, 0x03, 0x64, 0x65, 0x66, 0x00, 0x00, 0x00,
//            0x01, 0x3f, 0x00, 0x0c, 0x3f, 0x00, 0x00, 0x00,
//            0x00, 0x00, 0xfd, 0x80, 0x00, 0x00, 0x00, 0x00,
//            0x05, 0x00, 0x00, 0x0a, 0xfe, 0x00, 0x00, 0x02,
//            0x00};

}

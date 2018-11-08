package io.mycat.mycat2.bufferTest;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.buffer.DirectByteBufferPool;
import io.mycat.util.StringUtil;

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
        //在写入两个整包,并读出
        testFullFullPacket();
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
        mySQLSession.bufPool= new DirectByteBufferPool((1024*1024*4),(short)(1024*4*2),(short)64);
        
        while (mySQLSession.isResolveMySQLPackageFinished()) {
            AbstractMySQLSession.CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage();
            if(currPacketType == AbstractMySQLSession.CurrPacketType.Full){
                 System.out.println("-----FullPacket---");
                 System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
            }
            if(currPacketType == AbstractMySQLSession.CurrPacketType.LongHalfPacket){
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
    		    0x6c, 0x65, 0x73, 0x3b
    	  };
    	ByteBuffer buffer = ByteBuffer.allocate(8192);
        ProxyBuffer proxyBuffer = new ProxyBuffer(buffer);
        System.out.println(peer.length);
        for (int i : peer) {
            proxyBuffer.writeByte((byte) i);
        }

        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        while (mySQLSession.isResolveMySQLPackageFinished()) {
          AbstractMySQLSession.CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage();
          if(currPacketType == AbstractMySQLSession.CurrPacketType.Full){
               System.out.println("-----FullPacket---");
               System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
          }
          if(currPacketType == AbstractMySQLSession.CurrPacketType.ShortHalfPacket){
              System.out.println("-----ShortHalfPacket---");
              System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));

              //再次写入剩下的包 查看proxyBuffer是否异常
              int[] peer1 = new int[]{ 
                  0x0d, 0x00, 0x00, 0x00, 0x03, 0x73, 0x68, 0x6f, 0x77, 0x20, 0x74, 0x61, 0x62
                };
              //写入测试
              for (int i : peer1) {
                  proxyBuffer.writeByte((byte) i);
              }
          }        
      }
    }
    
    //ShortHalf
    //测试传输了result Set首包,大于251时
    @Test
    public void testResultSetShortHalfPacket() {
    	int[] peer = new int[]{ 
    			   0x01, 0x00, 0x00, 0x01, 0xfc
    	  };
    	ByteBuffer buffer = ByteBuffer.allocate(8192);
        ProxyBuffer proxyBuffer = new ProxyBuffer(buffer);
        System.out.println(peer.length);
        //写入一个short包
        for (int i : peer) {
            proxyBuffer.writeByte((byte) i);
        }
        //读一个short包
        AbstractMySQLSession mySQLSession = mock(proxyBuffer);
        while (mySQLSession.isResolveMySQLPackageFinished()) {
          AbstractMySQLSession.CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage();
          if(currPacketType == AbstractMySQLSession.CurrPacketType.Full){
               System.out.println("-----FullPacket---");
               System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
          }
          if(currPacketType == AbstractMySQLSession.CurrPacketType.ShortHalfPacket){
              System.out.println("-----ShortHalfPacket---");
              System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));

              //再次写入剩下的包 查看proxyBuffer是否异常
              int[] peer1 = new int[]{ 
                  0x01
                };
              //写入测试
              for (int i : peer1) {
                  proxyBuffer.writeByte((byte) i);
              }
          }        
      }
    }
    
    /**
     *   FullLongHalf测试
     *   完整包
     *   01 00 00 01 05
     *   2b 00 00 02 03 64 65 66 03 64 62 32 07 6d 65 73
   	 *	 73 61 67 65 07 6d 65 73 73 61 67 65 02 69 64 02
   	 *   69 64 0c 3f 00 0b 00 00 00 03 0b 42 00 00 00
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
        mySQLSession.bufPool= new DirectByteBufferPool((1024*1024*4),(short)(1024*4*2),(short)64);
        while (mySQLSession.isResolveMySQLPackageFinished()) {
            AbstractMySQLSession.CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage();
            if(currPacketType == AbstractMySQLSession.CurrPacketType.Full){
                 System.out.println("-----FullPacket---");
            	 System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
            }
            if(currPacketType == AbstractMySQLSession.CurrPacketType.LongHalfPacket){
                System.out.println("-----LongHalfPacket---");
           	 	System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));

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
     *   FullShortHalf测试  有错
     *   完整包
     *   07 00 00 11 01 39 00 fb 01 39 fb
     *   05 00 00 12 fe 00 00 22 00
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
            AbstractMySQLSession.CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage();
            if(currPacketType == AbstractMySQLSession.CurrPacketType.Full){
                 System.out.println("-----FullPacket---");
                 System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
            }
            if (currPacketType == AbstractMySQLSession.CurrPacketType.ShortHalfPacket) {
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
     *   FullFull测试 
     *   完整包
     *   01 00 00 01 05
     *   2b 00 00 02 03 64 65 66 03 64 62 32 07 6d 65 73
     *   73 61 67 65 07 6d 65 73 73 61 67 65 02 69 64 02
     *   69 64 0c 3f 00 0b 00 00 00 03 0b 42 00 00 00
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
            AbstractMySQLSession.CurrPacketType currPacketType = mySQLSession.resolveMySQLPackage();
            Assert.assertEquals(currPacketType, AbstractMySQLSession.CurrPacketType.Full);
            System.out.println(StringUtil.dumpAsHex(mySQLSession.proxyBuffer.getBuffer(), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.pkgLength));
        }
        //再次写入一个整包,并读出
        testFullPacket();
    }
    
    
  int[] peer1_23 = new int[] { /* Packet 175628 */
		  0x0d, 0x00, 0x00, 0x00, 0x03, 0x73, 0x68, 0x6f, 0x77, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73,
	      0x3b
  };
    
}

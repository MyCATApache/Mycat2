package io.mycat.mycat2;

import io.mycat.mysql.packet.*;
import io.mycat.proxy.ProxyBuffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * cjw
 * 294712221@qq.com
 */
public class TestUtil {
    public static byte[] of(int... i) {
        byte[] bytes = new byte[i.length];
        int j = 0;
        for (int i1 : i) {
            bytes[j] = (byte) i1;
            j++;
        }
        return bytes;
    }

    public static ProxyBuffer ofBuffer(int... i) {
        ProxyBuffer proxyBuffer = new ProxyBuffer(ByteBuffer.wrap(of(i)));
        proxyBuffer.writeIndex = i.length;
        return proxyBuffer;
    }

    public static ProxyBuffer exampleBuffer() {
        ByteBuffer allocate = ByteBuffer.allocate(128);
        return new ProxyBuffer(allocate);
    }

    public static ProxyBuffer eof(int serverStatus) {
        EOFPacket eofPacket = new EOFPacket();
        ProxyBuffer buffer = exampleBuffer();
        eofPacket.status = serverStatus;
        eofPacket.write(buffer);
        return buffer;
    }

    public static ProxyBuffer ok(int serverStatus) {
        OKPacket okPacket = new OKPacket();
        ProxyBuffer buffer = exampleBuffer();
        okPacket.serverStatus = serverStatus;
        okPacket.write(buffer);
        return buffer;
    }

    public static ProxyBuffer err() {
        ErrorPacket eofPacket = new ErrorPacket();
        ProxyBuffer buffer = exampleBuffer();
        eofPacket.write(buffer);
        return buffer;
    }
    public static ProxyBuffer fieldCount() {
        ResultSetHeaderPacket headerPacket = new ResultSetHeaderPacket();
        ProxyBuffer buffer = exampleBuffer();
        headerPacket.write(buffer);
        return buffer;
    }
    public static ProxyBuffer field() {
        FieldPacket fieldPacket = new FieldPacket();
        ProxyBuffer buffer = exampleBuffer();
        fieldPacket.write(buffer);
        return buffer;
    }

    public static ProxyBuffer row(int field) {
        RowDataPacket rowDataPacket = new RowDataPacket(field);
        ProxyBuffer buffer = exampleBuffer();
        rowDataPacket.write(buffer);
        return buffer;
    }
    
    /**
     * yushuozhu
     * 1289303556@qq.com
     * 
     * Convert hex string to byte[]
     * 
     * @param hex stream files File path
     * @return byte[]
     */
    public static byte[] hexStringToBytes(String filePath) {
      
      try {
        RandomAccessFile fos = new RandomAccessFile(filePath,"rw");
        FileChannel fc = fos.getChannel();
        final MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, fos.length());
        
        byte[] bytes1 = new byte[(int) fos.length()];
        for (int i = 0; i < fos.length(); i++) {  
            bytes1[i] = mbb.get(i);
        } 
        String hexString = new String(bytes1);
        byte[] bytes = new byte[hexString.length() / 2];
        for (int i = 0; i < hexString.length() / 2; i++) {
          String temp = hexString.substring(i * 2, (i + 1) * 2);
          byte v = (byte) Integer.parseInt(temp, 16);
          bytes[i] = v;
        } 
        return bytes;
      } catch (NumberFormatException e) {
        e.printStackTrace();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
    
    
    public static void main(String[] args) {
      /**
       * F:/mycat/hex.txt内容为hex流    0700000200000002000000
       */
      System.out.println(new String(hexStringToBytes("F:/mycat/hex.txt")));
    }
}

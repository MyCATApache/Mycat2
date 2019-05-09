package io.mycat.util;

/**
 * @author jamie12221
 * @date 2019-05-03 14:01
 **/
public class MySQLUtil {

  public static byte[] getFixIntByteArray(int length, long val) {
    byte[] bytes = new byte[length];
    int index = 0;
    for (int i = 0; i < length; i++) {
      byte b = (byte) ((val >>> (i * 8)) & 0xFF);
      bytes[index++] = b;
    }
    return bytes;
  }

}

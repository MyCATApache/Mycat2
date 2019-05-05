package io.mycat.sqlparser.util;

/**
 * Created by jamie on 2017/8/29.
 */
public interface ByteArrayView {

  byte getByte(int index);

  int length();


  public void set(int index, byte value);


  public void setOffset(int offset);

  default String getString(int start, int length) {
    return new String(getBytes(start, length));
  }

  default String getStringByHashArray(int pos, HashArray hashArray) {
    String res = this.getString(hashArray.getPos(pos), hashArray.getSize(pos));
    return res;
  }

  public default byte[] getBytes(int start, int length) {
    byte[] bytes = new byte[length];
    for (int i = start, j = 0; j < length; i++, j++) {
      bytes[j] = getByte(i);
    }
    return bytes;
  }

  int getOffset();
}

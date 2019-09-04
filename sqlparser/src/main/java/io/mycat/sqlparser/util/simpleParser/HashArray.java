package io.mycat.sqlparser.util.simpleParser;

/**
 * Created by Kaiz on 2017/3/21. 2017/11/25: 调整hasharray大小，从4096增加到8192，如果要调整大小，需要调整 Context
 * 中sqlInfo的 preHashPos 和 SQLSize 大小 2017/11/26: 调整hashArray排列 63........................................................0
 * |----24----|-----32------|---8---| pos         type        size 这样设计是考虑到减少最常用的部分（取intTokenHash）的运算开销
 */
public class HashArray {

  long[] hashArray;
  int pos = 0;

  public HashArray() {
    hashArray = new long[1024];
  }

  public HashArray(int size) {
    hashArray = new long[size];
  }

  public void init() {
    while (pos > 0) {
      hashArray[pos--] = 0;
    }
    pos = 0;
  }

  public void init(int length) {
    if (hashArray.length < length) {
      hashArray = new long[length];
    } else {
      if (hashArray.length > 1024) {
        hashArray = new long[length];
      }else {
        while (pos > 0) {
          hashArray[pos--] = 0;
        }
      }
    }
    pos = 0;
  }

  public void set(int type, int start, int size) {
    set(type, start, size, 0L);
  }

  public void set(int type, int start, int size, long hash) {
    size = size < 0xFF ? size : 0xFF;
    try {
      hashArray[pos++] =
          (((long) start) << 40) & 0xFFFFFF0000000000L | ((long) type << 8) & 0xFFFFFFFF00L
              | (long) size;
      hashArray[pos++] = hash;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public int getPos(int idx) {
    return (int) ((hashArray[idx << 1] >>> 40) & 0xFFFFFF);
  }

  public int getSize(int idx) {
    int size = ((int) hashArray[idx << 1] & 0xFF);
    if (size >= 0xFF) {
      size = getPos(idx + 1) - getPos(idx);
    }
    return size;
  }

  public int getType(int idx) {
    return (int) (hashArray[idx << 1] >>> 8);
  }

  public void setType(int idx, int type) {
    hashArray[idx << 1] = (hashArray[idx << 1] & 0xFFFFFF00000000FFL) | ((long) type << 8);
  }

  public long getHash(int idx) {
    return hashArray[(idx << 1) + 1];
  }

  public int getIntHash(int idx) {
    return (int) hashArray[idx << 1];
  }

  public int getCount() {
    return pos >> 1;
  }
}

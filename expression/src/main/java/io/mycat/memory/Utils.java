package io.mycat.memory;

import java.nio.charset.StandardCharsets;

class Utils {
  public static byte[] getBytesFromUTF8String(String str) {
    return str.getBytes(StandardCharsets.UTF_8);
  }

  public static long integralToLong(Object i) {
    long longValue;

    if (i instanceof Long) {
      longValue = (Long) i;
    } else if (i instanceof Integer) {
      longValue = ((Integer) i).longValue();
    } else if (i instanceof Short) {
      longValue = ((Short) i).longValue();
    } else if (i instanceof Byte) {
      longValue = ((Byte) i).longValue();
    } else {
      throw new IllegalArgumentException("Unsupported data type " + i.getClass().getName());
    }

    return longValue;
  }

  public static String bytesToString(long released) {
    return Long.valueOf(released).toString();
  }
}
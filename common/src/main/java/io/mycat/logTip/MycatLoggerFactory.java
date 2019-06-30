package io.mycat.logTip;

public class MycatLoggerFactory {

  public static MycatLogger getLogger(Class<?> clazz) {
    return new MycatLogger(clazz);
  }

  public static MycatLogger getLogger(String name) {
    return new MycatLogger(name);
  }
}
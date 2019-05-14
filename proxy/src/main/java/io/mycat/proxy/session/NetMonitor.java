package io.mycat.proxy.session;

import io.mycat.util.DumpUtil;
import java.nio.ByteBuffer;

/**
 * @author jamie12221
 * @date 2019-05-14 11:51
 **/
public final class NetMonitor {

  final static boolean record = false;

  public final static void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {
    if (record) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  public final static void onBackendWrite(Session session, ByteBuffer view, int startIndex,
      int len) {
    if (record) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  public final static void onBackendRead(Session session, ByteBuffer view, int startIndex,
      int len) {
    if (record) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  public final static void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len) {
    if (record) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }
}

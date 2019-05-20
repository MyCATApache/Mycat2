package io.mycat.proxy.monitor;

import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import io.mycat.util.DumpUtil;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221
 * @date 2019-05-20 11:52
 **/
public class MycatMonitorLogCallback implements MycatMonitorCallback {

  protected final static Logger LOGGER = LoggerFactory.getLogger(MycatMonitor.class);
  final static boolean record = true;

  public final void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {
    if (record) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  public final void onBackendWrite(Session session, ByteBuffer view, int startIndex,
      int len) {
    if (record) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  public final void onBackendRead(Session session, ByteBuffer view, int startIndex,
      int len) {
    if (record) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  public final void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len) {
    if (record) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  public final void onMySQLSessionServerStatus(MySQLClientSession session) {
    if (record) {

    }
  }

  public final void onAllocateByteBuffer(ByteBuffer buffer) {
    if (record) {
      //    Thread.dumpStack();
      LOGGER.debug("{}  {}", MycatMonitorCallback.getSession(), buffer);
    }
  }

  public final void onSynchronizationState(MycatSession mycat, MySQLClientSession session) {

  }

  public final void onRecycleByteBuffer(ByteBuffer buffer) {
    if (record) {
      LOGGER.debug("{}  {}", MycatMonitorCallback.getSession(), buffer);
    }
  }

  public final void onExpandByteBuffer(ByteBuffer buffer) {
    if (record) {
      LOGGER.debug("{}  {}", MycatMonitorCallback.getSession(), buffer);
    }
  }

  public final void onNewMycatSession(MycatSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{} {}", mycat, session);
    }
  }

  public final void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{} {}", mycat, session);
    }
  }

  public final void onCloseMycatSession(MycatSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final void onNewMySQLSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final void onAddIdleMysqlSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final void onGetIdleMysqlSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final void onCloseMysqlSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

}

package io.mycat.proxy;

import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import io.mycat.util.DumpUtil;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221
 * @date 2019-05-14 11:51
 **/
public final class MycatMonitor {

  protected final static Logger LOGGER = LoggerFactory.getLogger(MycatMonitor.class);
  final static boolean record = true;

  public final static void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {
    if (record) {
      //DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  public final static void onBackendWrite(Session session, ByteBuffer view, int startIndex,
      int len) {
    if (record) {
      //  DumpUtil.printAsHex(view, startIndex, len);
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
      //  DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  public final static void onMySQLSessionServerStatus(MySQLClientSession session) {
    if (record) {

    }
  }

  public final static void onAllocateByteBuffer(ByteBuffer buffer) {
    if (record) {
      //    Thread.dumpStack();
      LOGGER.debug("{}  {}", getSession(), buffer);
    }
  }

  public final static void onRecycleByteBuffer(ByteBuffer buffer) {
    if (record) {
      LOGGER.debug("{}  {}", getSession(), buffer);
    }
  }
//  public final static void onAllocateOffHeapByteBuffer(Object id, ByteBuffer buffer) {
//    if (record) {
//
//    }
//  }


  public final static void onExpandByteBuffer(ByteBuffer buffer) {
    if (record) {
      LOGGER.debug("{}  {}", getSession(), buffer);
    }
  }

  //  public final static void onRecycleOffHeapByteBuffer(Object id,ByteBuffer buffer) {
//    if (record) {
//
//    }
//  }
  public final static void onNewMycatSession(MycatSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final static void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{} {}", mycat, session);
    }
  }

  public final static void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{} {}", mycat, session);
    }
  }

  public final static void onCloseMycatSession(MycatSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final static void onNewMySQLSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final static void onAddIdleMysqlSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final static void onGetIdleMysqlSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public final static void onCloseMysqlSession(MySQLClientSession session) {
    if (record) {
      LOGGER.debug("{}", session);
    }
  }

  public static MycatReactorThread getThread() {
    Thread thread = Thread.currentThread();
    MycatReactorThread thread1 = (MycatReactorThread) thread;
    return thread1;
  }

  public static Session getSession() {
    MycatReactorThread thread = getThread();
    Session curSession = thread.getReactorEnv().getCurSession();
    if (curSession instanceof MycatSession) {
      return curSession;
    } else if (curSession instanceof MySQLClientSession) {
      MycatSession mycatSession = ((MySQLClientSession) curSession).getMycatSession();
      if (mycatSession == null) {
        return curSession;
      } else {
        return mycatSession;
      }
    } else {
      return null;
    }
  }

}

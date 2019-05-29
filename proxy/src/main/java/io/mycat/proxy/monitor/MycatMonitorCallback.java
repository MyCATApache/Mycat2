package io.mycat.proxy.monitor;

import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import java.nio.ByteBuffer;

/**
 * @author jamie12221 date 2019-05-20 11:32
 **/
public interface MycatMonitorCallback {

  MycatMonitorCallback EMPTY = new MycatMonitorCallback() {
    @Override
    public void onOrginSQL(Session session, String sql) {

    }

    @Override
    public void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {

    }

    @Override
    public void onBackendWrite(Session session, ByteBuffer view, int startIndex, int len) {

    }

    @Override
    public void onBackendRead(Session session, ByteBuffer view, int startIndex, int len) {

    }

    @Override
    public void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len) {

    }

    @Override
    public void onMySQLSessionServerStatusChanged(Session session, int serverStatus) {

    }

    @Override
    public void onAllocateByteBuffer(ByteBuffer buffer) {

    }

    @Override
    public void onSynchronizationState(MySQLClientSession session) {

    }


    @Override
    public void onRecycleByteBuffer(ByteBuffer buffer) {

    }

    @Override
    public void onExpandByteBuffer(ByteBuffer buffer) {

    }

    @Override
    public void onNewMycatSession(MycatSession session) {

    }

    @Override
    public void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {

    }

    @Override
    public void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {

    }

    @Override
    public void onCloseMycatSession(MycatSession session) {

    }

    @Override
    public void onNewMySQLSession(MySQLClientSession session) {

    }

    @Override
    public void onAddIdleMysqlSession(MySQLClientSession session) {

    }

    @Override
    public void onGetIdleMysqlSession(MySQLClientSession session) {

    }

    @Override
    public void onCloseMysqlSession(MySQLClientSession session) {

    }

    @Override
    public void onRoute(Session session, String dataNode, byte[] payload) {

    }
  };

  static MycatReactorThread getThread() {
    Thread thread = Thread.currentThread();
    MycatReactorThread thread1 = (MycatReactorThread) thread;
    return thread1;
  }

  static Session getSession() {
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

  void onOrginSQL(Session session, String sql);

  void onFrontRead(Session session, ByteBuffer view, int startIndex, int len);

  void onBackendWrite(Session session, ByteBuffer view, int startIndex,
      int len);

  void onBackendRead(Session session, ByteBuffer view, int startIndex,
      int len);

  void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len);

  void onMySQLSessionServerStatusChanged(Session session, int serverStatus);

  void onAllocateByteBuffer(ByteBuffer buffer);

  void onSynchronizationState(MySQLClientSession session) ;
  void onRecycleByteBuffer(ByteBuffer buffer) ;
  void onExpandByteBuffer(ByteBuffer buffer);

  void onNewMycatSession(MycatSession session);

  void onBindMySQLSession(MycatSession mycat, MySQLClientSession session);

  void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) ;
  void onCloseMycatSession(MycatSession session) ;
  void onNewMySQLSession(MySQLClientSession session) ;
  void onAddIdleMysqlSession(MySQLClientSession session) ;
  void onGetIdleMysqlSession(MySQLClientSession session) ;

  void onCloseMysqlSession(MySQLClientSession session);

  void onRoute(Session session, String dataNode, byte[] payload) ;
}

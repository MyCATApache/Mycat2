package io.mycat.proxy.monitor;

import static io.mycat.proxy.monitor.MycatMonitorCallback.EMPTY;

import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author jamie12221
 *  date 2019-05-14 11:51
 **/
public final class MycatMonitor {

  static MycatMonitorCallback callback = EMPTY;

  public final static void onOrginSQL(Session session, String sql) {
    Objects.requireNonNull(session);
    Objects.requireNonNull(sql);
    callback.onOrginSQL(session, sql);
  }

  public final static void onRoute(Session session, String dataNode, byte[] packet) {
    Objects.requireNonNull(session);
    Objects.requireNonNull(dataNode);
    Objects.requireNonNull(packet);
    callback.onRoute(session, dataNode, packet);
  }
  public static void setCallback(MycatMonitorCallback callback) {
    Objects.requireNonNull(callback);
    MycatMonitor.callback = callback;
  }

  public final static void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {
    callback.onFrontRead(session, view, startIndex, len);
  }

  public final static void onBackendWrite(Session session, ByteBuffer view, int startIndex,
      int len) {
    callback.onBackendWrite(session, view, startIndex, len);
  }

  public final static void onBackendRead(Session session, ByteBuffer view, int startIndex,
      int len) {
    callback.onBackendRead(session, view, startIndex, len);
  }

  public final static void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len) {
    callback.onFrontWrite(session, view, startIndex, len);
  }

  public final static void onMySQLSessionServerStatusChanged(Session session, int serverStatus) {
    callback.onMySQLSessionServerStatusChanged(session, serverStatus);
  }

  public final static void onSynchronizationState(MySQLClientSession session) {
    callback.onSynchronizationState(session);
  }
  public final static void onAllocateByteBuffer(ByteBuffer buffer) {
    callback.onAllocateByteBuffer(buffer);
  }

  public final static void onRecycleByteBuffer(ByteBuffer buffer) {
    callback.onRecycleByteBuffer(buffer);
  }

  public final static void onExpandByteBuffer(ByteBuffer buffer) {
    callback.onExpandByteBuffer(buffer);
  }

  public final static void onNewMycatSession(MycatSession session) {
    callback.onNewMycatSession(session);
  }

  public final static void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    callback.onBindMySQLSession(mycat, session);
  }

  public final static void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    callback.onUnBindMySQLSession(mycat, session);
  }

  public final static void onCloseMycatSession(MycatSession session) {
    callback.onCloseMycatSession(session);
  }

  public final static void onNewMySQLSession(MySQLClientSession session) {
    callback.onNewMySQLSession(session);
  }

  public final static void onAddIdleMysqlSession(MySQLClientSession session) {
    callback.onAddIdleMysqlSession(session);
  }

  public final static void onGetIdleMysqlSession(MySQLClientSession session) {
    callback.onGetIdleMysqlSession(session);
  }

  public final static void onCloseMysqlSession(MySQLClientSession session) {
    callback.onCloseMysqlSession(session);
  }

}

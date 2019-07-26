package io.mycat.proxy.monitor;

import io.mycat.MycatException;
import io.mycat.annotations.NoExcept;
import io.mycat.proxy.handler.backend.MySQLSynContext;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * @author jamie12221 date 2019-05-20 11:32
 **/
@NoExcept
public interface MycatMonitorCallback {

  MycatMonitorCallback EMPTY = new AbstractMonitorCallback() {

  };

  static MycatReactorThread getThread() {
    Thread thread = Thread.currentThread();
    return (MycatReactorThread) thread;
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
      throw new MycatException("unknown session");
    }
  }

  void onOrginSQL(Session session, String sql);

  void onFrontRead(Session session, ByteBuffer view, int startIndex, int len);

  void onPayloadType(Session session, MySQLPayloadType type
      //,ByteBuffer view, int startIndex, int len
  );

  void onBackendWrite(Session session, ByteBuffer view, int startIndex,
      int len);

  void onBackendRead(Session session, ByteBuffer view, int startIndex,
      int len);

  void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len);

  void onMySQLSessionServerStatusChanged(Session session, int serverStatus);

  void onAllocateByteBuffer(ByteBuffer buffer, Session session);

  void onSyncSQL(MySQLSynContext mycatSession, String sql, MySQLClientSession session);

  void onSynchronizationState(MySQLClientSession session);

  void onRecycleByteBuffer(ByteBuffer buffer, Session session);

  void onExpandByteBuffer(ByteBuffer buffer, Session session);

  void onNewMycatSession(MycatSession session);

  void onBindMySQLSession(MycatSession mycat, MySQLClientSession session);

  void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session);


  void onNewMySQLSession(MySQLClientSession session);

  void onAddIdleMysqlSession(MySQLClientSession session);

  void onGetIdleMysqlSession(MySQLClientSession session);
  void onRouteSQL(Session session, String dataNodeName,String sql);
  void onRouteSQLResult(Session session, String dataNodeName, String replicaName, String dataSource,
      byte[] payload);

  void onRouteSQLResult(Session session, String dataNodeName, String replicaName, String dataSource,
      String sql);
  /**
   * exception
   */
  void onBackendConCreateWriteException(Session session, Exception e);

  void onBackendConCreateConnectException(Session session, Exception e);

  void onBackendConCreateReadException(Session session, Exception e);

  void onBackendConCreateClear(Session session);

  void onBackendResultSetReadException(Session session, Exception e);

  void onBackendResultSetWriteException(Session session, Exception e);

  void onBackendResultSetWriteClear(Session session);

  void onResultSetWriteException(Session session, Exception e);

  void onResultSetReadException(Session session, Exception e);

  void onResultSetClear(Session session);

  void onIdleReadException(Session session, Exception e);

  void onAuthHandlerWriteException(Session session, Exception e);

  void onAuthHandlerReadException(Session session, Exception e);

  void onAuthHandlerClear(Session session);

  void onPacketExchangerWriteException(Session session, Exception e);

  void onPacketExchangerException(Session session, Exception e);

  void onPacketExchangerClear(Session session);

  void onMycatHandlerWriteException(Session session, Exception e);

//  void onMycatHandlerExchangerException(Session session, Exception e);

  void onMycatHandlerClear(Session session);

  void onCloseMysqlSession(MySQLClientSession session, boolean noraml, String reson);


  void onRequestException(MySQLClientSession session, Exception e);

  void onRequestClear(MySQLClientSession session);

  void onBackendConCreateException(Session session, Exception e);

  void onResultSetException(MySQLClientSession session, Exception e);

  void onMycatHandlerReadException(MycatSession mycat, Exception e);

  void onMycatHandlerException(MycatSession mycat, Exception e);

  void onMycatHandlerCloseException(MycatSession mycat, ClosedChannelException e);

  void onMycatServerWriteException(MycatSession session, Exception e);

  void onGettingBackend(Session session, String dataNode, Exception e);

  void onCloseMycatSession(MycatSession mycat, boolean normal, String reason);

  //command
  void onCommandStart(MycatSession mycat);

  void onCommandEnd(MycatSession mycat);

  void onSleepCommandStart(MycatSession mycat);

  void onSleepCommandEnd(MycatSession mycat);

  void onQuitCommandStart(MycatSession mycat);

  void onQuitCommandEnd(MycatSession mycat);

  void onQueryCommandStart(MycatSession mycat);

  void onQueryCommandEnd(MycatSession mycat);

  void onInitDbCommandStart(MycatSession mycat);

  void onInitDbCommandEnd(MycatSession mycat);

  void onPingCommandStart(MycatSession mycat);

  void onPingCommandEnd(MycatSession mycat);

  void onFieldListCommandStart(MycatSession mycat);

  void onFieldListCommandEnd(MycatSession mycat);

  void onSetOptionCommandStart(MycatSession mycat);

  void onSetOptionCommandEnd(MycatSession mycat);

  void onPrepareCommandStart(MycatSession mycat);

  void onPrepareCommandEnd(MycatSession mycat);

  void onSendLongDataCommandStart(MycatSession mycat);

  void onSendLongDataCommandEnd(MycatSession mycat);

  void onExecuteCommandStart(MycatSession mycat);

  void onExecuteCommandEnd(MycatSession mycat);

  void onCloseCommandStart(MycatSession mycat);

  void onCloseCommandEnd(MycatSession mycat);

  void onResetCommandStart(MycatSession mycat);

  void onResetCommandEnd(MycatSession mycat);

  void onCreateDbCommandStart(MycatSession mycat);

  void onCreateDbCommandEnd(MycatSession mycat);


  void onDropDbCommandStart(MycatSession mycat);

  void onDropDbCommandEnd(MycatSession mycat);

  void onRefreshCommandStart(MycatSession mycat);

  void onRefreshCommandEnd(MycatSession mycat);

  void onShutdownCommandStart(MycatSession mycat);

  void onStatisticsCommandEnd(MycatSession mycat);

  void onProcessInfoCommandStart(MycatSession mycat);

  void onProcessInfoCommandEnd(MycatSession mycat);

  void onConnectCommandStart(MycatSession mycat);

  void onConnectCommandEnd(MycatSession mycat);

  void onProcessKillCommandStart(MycatSession mycat);

  void onProcessKillCommandEnd(MycatSession mycat);

  void onDebugCommandStart(MycatSession mycat);

  void onDebugCommandEnd(MycatSession mycat);

  void onTimeCommandStart(MycatSession mycat);

  void onTimeCommandEnd(MycatSession mycat);

  void onDelayedInsertCommandStart(MycatSession mycat);

  void onDelayedInsertCommandEnd(MycatSession mycat);

  void onChangeUserCommandStart(MycatSession mycat);

  void onChangeUserCommandEnd(MycatSession mycat);

  void onResetConnectionCommandStart(MycatSession mycat);

  void onResetConnectionCommandEnd(MycatSession mycat);

  void onDaemonCommandStart(MycatSession mycat);

  void onDaemonCommandEnd(MycatSession mycat);

  void onShutdownCommandEnd(MycatSession mycat);

  void onStatisticsCommandStart(MycatSession mycat);

  void onPacketExchangerRead(Session session);

  void onPacketExchangerWrite(Session session);

  void onFetchCommandStart(MycatSession mycat);

  void onFetchCommandEnd(MycatSession mycat);

  void onLoadDataLocalInFileEmptyPacketStart(MycatSession mycat);

  void onLoadDataLocalInFileEmptyPacketEnd(MycatSession mycat);

  void onLoadDataLocalInFileContextStart(MycatSession mycat);

  void onLoadDataLocalInFileContextEnd(MycatSession mycat);

  void onChange2ReadOpts(Session session);

  void onChange2WriteOpts(Session session);

  void onClearReadWriteOpts(Session session);

  void onResultSetEnd(MySQLClientSession mysql);
}

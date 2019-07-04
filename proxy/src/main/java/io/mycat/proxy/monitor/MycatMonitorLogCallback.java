package io.mycat.proxy.monitor;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.handler.backend.MySQLSynContext;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import io.mycat.util.DumpUtil;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * @author jamie12221 date 2019-05-20 11:52
 **/
public class MycatMonitorLogCallback implements MycatMonitorCallback {

  protected final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatMonitor.class);
  protected final static MycatLogger SQL_LOGGER = MycatLoggerFactory.getLogger("sqlLogger");
  final static boolean onBind = true;
  final static boolean onSessionPool = true;
  final static boolean onBuffer = true;
  final static boolean recordDump = true;
  final static boolean onSQL = true;
  final static boolean onException = true;
  final static boolean onClear = true;
  final static boolean onCommand = true;
  final static boolean onBackend = true;
  final static boolean onPayload = true;
  final static boolean readWriteOpts = true;
  @Override
  public void onMySQLSessionServerStatusChanged(Session session, int serverStatus) {
    if (onBind) {

      boolean hasFatch = MySQLPacketResolver.hasFatch(serverStatus);
      boolean hasMoreResult = MySQLPacketResolver.hasMoreResult(serverStatus);
      boolean hasTranscation = MySQLPacketResolver.hasTrans(serverStatus);
      LOGGER.info("sessionId:{}  serverStatus:{} hasFatch:{} hasMoreResult:{} hasTranscation:{}",
          session.sessionId(), serverStatus, hasFatch, hasMoreResult, hasTranscation);
    }
  }

  @Override
  public void onOrginSQL(Session session, String sql) {
    if (onSQL) {
      SQL_LOGGER.info("sessionId:{}  orginSQL:{} ", session.sessionId(), sql);
    }
  }

  @Override
  public void onRoute(Session session, String dataNode, byte[] payload) {
    if (onSQL) {
      SQL_LOGGER.info("sessionId:{} dataNode:{}  payload:{} ", session.sessionId(), dataNode,
          new String(payload));
    }
  }

  /**
   * exception
   */
  @Override
  public void onBackendConCreateWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onBackendConCreateConnectException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onBackendConCreateReadException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onBackendConCreateClear(Session session) {
    if (onClear) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onBackendResultSetReadException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onBackendResultSetWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onBackendResultSetWriteClear(Session session) {
    if (onClear) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onResultSetWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onResultSetReadException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onResultSetClear(Session session) {
    if (onClear) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onIdleReadException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onAuthHandlerWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onAuthHandlerReadException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onAuthHandlerClear(Session session) {
    if (onClear) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onPacketExchangerWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onPacketExchangerException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onPacketExchangerClear(Session session) {
    if (onClear) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onMycatHandlerWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onMycatHandlerClear(Session session) {
    if (onClear) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onCloseMysqlSession(MySQLClientSession session, boolean normal, String reason) {
    if (onSessionPool) {
      LOGGER.info("sessionId:{} normal:{} message:{}", session.sessionId(), normal, reason);
    }
  }


  @Override
  public void onRequestException(MySQLClientSession session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onRequestClear(MySQLClientSession session) {
    if (onClear) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onBackendConCreateException(Session session, Exception e) {
    if (onException && session != null) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onResultSetException(MySQLClientSession session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onMycatHandlerReadException(MycatSession mycat, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onMycatHandlerException(MycatSession mycat, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onMycatHandlerCloseException(MycatSession mycat, ClosedChannelException e) {
    if (onException) {
      LOGGER.info("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onMycatServerWriteException(MycatSession session, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onGettingBackend(Session session, String dataNode, Exception e) {
    if (onException) {
      LOGGER.info("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public final void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {
    if (recordDump) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  @Override
  public void onPayloadType(Session session, MySQLPayloadType type) {
    if (onPayload) {
      LOGGER.debug("sessionId:{} payload:{} ", session.sessionId(), type);
    }
  }

  @Override
  public final void onBackendWrite(Session session, ByteBuffer view, int startIndex,
      int len) {
    if (recordDump) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  @Override
  public final void onBackendRead(Session session, ByteBuffer view, int startIndex,
      int len) {
    if (recordDump) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  @Override
  public final void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len) {
    if (recordDump) {
      DumpUtil.printAsHex(view, startIndex, len);
    }
  }

  @Override
  public final void onAllocateByteBuffer(ByteBuffer buffer, Session session) {
    if (onBuffer) {
      //    Thread.dumpStack();
      LOGGER.debug("session id{}  {}", session.sessionId(), buffer);
    }
  }

  @Override
  public void onSyncSQL(MySQLSynContext context, String sql, MySQLClientSession session) {
    if (onSQL) {
      //    Thread.dumpStack();
      SQL_LOGGER.debug("sync mysql session:{} sql:{}", session.sessionId(), sql);
    }
  }

  @Override
  public final void onSynchronizationState(MySQLClientSession session) {
    if (onBind) {
      //    Thread.dumpStack();
      MySQLSynContext c = new MySQLSynContext(session);
      LOGGER.debug(
          "sessionId:{} dataNode:{} isolation: {} charset:{} automCommit:{} characterSetResult:{} sqlSelectLimit:{} netWriteTimeout:{}",
          session.sessionId(), c.getDataNode(),
          c.getIsolation(), c.getCharset(), c.getAutoCommit(),
          c.getCharacterSetResult(),c.getSqlSelectLimit(),c.getNetWriteTimeout());
    }
  }

  @Override
  public final void onRecycleByteBuffer(ByteBuffer buffer, Session session) {
    if (onBuffer) {
      LOGGER.debug("sessionId:{}  {}", MycatMonitorCallback.getSession().sessionId(), buffer);
    }
  }

  @Override
  public final void onExpandByteBuffer(ByteBuffer buffer, Session session) {
    if (onBuffer) {
      LOGGER.debug("sessionId:{}  {}", MycatMonitorCallback.getSession().sessionId(), buffer);
    }
  }

  @Override
  public final void onNewMycatSession(MycatSession session) {
    if (onSessionPool) {
      LOGGER.debug("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public final void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (onBind) {
      LOGGER.debug("sessionId:{} sessionId:{}", mycat.sessionId(), session.sessionId());
    }
  }

  @Override
  public final void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (onBind) {
      LOGGER.debug("sessionId:{} sessionId:{}", mycat.sessionId(), session.sessionId());
    }
  }

  @Override
  public final void onCloseMycatSession(MycatSession session, boolean normal, String reason) {
    if (onSessionPool) {
      LOGGER.debug("sessionId:{} normal:{} message:{}", session.sessionId(), normal, reason);
    }
  }

  @Override
  public final void onNewMySQLSession(MySQLClientSession session) {
    if (onSessionPool) {
      LOGGER.debug("sessionId:{} dataSourceName:{}", session.sessionId(),
          session.getDatasource().getName());
    }
  }

  @Override
  public final void onAddIdleMysqlSession(MySQLClientSession session) {
    if (onSessionPool) {
      LOGGER.debug("sessionId:{} dataSourceName:{}", session.sessionId(),
          session.getDatasource().getName());
    }
  }

  @Override
  public final void onGetIdleMysqlSession(MySQLClientSession session) {
    if (onSessionPool) {
      LOGGER.debug("sessionId:{} dataSourceName:{}", session.sessionId(),
          session.getDatasource().getName());
    }
  }

  @Override
  public void onCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSleepCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSleepCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onQuitCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onQuitCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onQueryCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onQueryCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onInitDbCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onInitDbCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onPingCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onPingCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onFieldListCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onFieldListCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSetOptionCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSetOptionCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onPrepareCommandStart(MycatSession mycat) {

  }

  @Override
  public void onPrepareCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSendLongDataCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSendLongDataCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onExecuteCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onExecuteCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onCloseCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onCloseCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onResetCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onResetCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onCreateDbCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onCreateDbCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onDropDbCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onDropDbCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onRefreshCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onRefreshCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onShutdownCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onStatisticsCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onProcessInfoCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onProcessInfoCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onConnectCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onConnectCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onProcessKillCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onProcessKillCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onDebugCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onDebugCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onTimeCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onTimeCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onDelayedInsertCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onDelayedInsertCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onChangeUserCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onChangeUserCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onResetConnectionCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onResetConnectionCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onDaemonCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onDaemonCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onShutdownCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onStatisticsCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onPacketExchangerRead(Session session) {
    if (onBackend) {
      LOGGER.debug("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onPacketExchangerWrite(Session session) {
    if (onBackend) {
      LOGGER.debug("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onFetchCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onFetchCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onLoadDataLocalInFileEmptyPacketStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onLoadDataLocalInFileEmptyPacketEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onLoadDataLocalInFileContextStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onLoadDataLocalInFileContextEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onChange2ReadOpts(Session session) {
    if (readWriteOpts) {
      LOGGER.debug("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onChange2WriteOpts(Session session) {
    if (readWriteOpts) {
      LOGGER.debug("sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onClearReadWriteOpts(Session session) {
    if (readWriteOpts) {
      LOGGER.debug("sessionId:{}", session.sessionId());
    }
  }
}

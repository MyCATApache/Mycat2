/**
 * Copyright (C) <2020>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.monitor;

import io.mycat.proxy.handler.backend.MySQLSynContext;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import io.mycat.util.DumpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * @author jamie12221 date 2019-05-20 11:52
 **/
public class MycatMonitorLogCallback implements MycatMonitorCallback {

  static final Logger LOGGER = LoggerFactory.getLogger(MycatMonitorLogCallback.class);
  private static final Logger SQL_LOGGER = LoggerFactory.getLogger("sqlLogger");
  final static boolean onBind = false;
  final static boolean onSessionPool = false;
  final static boolean onBuffer = false;
  final static boolean recordDump = false;
  final static boolean onSQL = true;
  final static boolean onException = true;
  final static boolean onClear = false;
  final static boolean onCommand = false;
  final static boolean onBackend = false;
  final static boolean onPayload = false;
  final static boolean readWriteOpts = false;

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
//
//  @Override
//  public void onRouteSQL(Session session, String dataNodeName, String sql) {
//    if (onSQL) {
//      SQL_LOGGER.info("sessionId:{} dataTarget:{} sql:{}", session.sessionId(), dataNodeName, sql);
//    }
//  }

  @Override
  public void onRouteSQLResult(Session session, String dataNodeName, String defaultDataBase,
      String dataSourceName,
      byte[] payload) {
    if (onSQL) {
      SQL_LOGGER.info("sessionId:{} dataTarget:{} replica:{} datasource:{}", session.sessionId(), dataNodeName,
              defaultDataBase,dataSourceName);
    }
  }

  @Override
  public void onRouteSQLResult(Session session, String dataNodeName, String defaultDataBase,
      String dataSourceName, String sql) {
    if (onSQL) {
      SQL_LOGGER.info("sessionId:{} dataTarget:{} replica:{} datasource:{}", session.sessionId(),
          dataNodeName,
              defaultDataBase, dataSourceName);
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
      LOGGER.info("onBackendConCreateReadException sessionId:{} exception:{}", session.sessionId(),
          e);
    }
  }

  @Override
  public void onBackendConCreateClear(Session session) {
    if (onClear) {
      LOGGER.info("onBackendConCreateClear sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onBackendResultSetReadException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onBackendResultSetReadException sessionId:{} exception:{}", session.sessionId(),
          e);
    }
  }

  @Override
  public void onBackendResultSetWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onBackendResultSetWriteException sessionId:{} exception:{}", session.sessionId(),
          e);
    }
  }

  @Override
  public void onBackendResultSetWriteClear(Session session) {
    if (onClear) {
      LOGGER.info("onBackendResultSetWriteClear sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onResultSetWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onResultSetWriteException sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onResultSetReadException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onResultSetReadException sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onResultSetClear(Session session) {
    if (onClear) {
      LOGGER.info("onResultSetClear sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onIdleReadException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onIdleReadException sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onAuthHandlerWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onAuthHandlerWriteException sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onAuthHandlerReadException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onAuthHandlerReadException sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onAuthHandlerClear(Session session) {
    if (onClear) {
      LOGGER.info("onAuthHandlerClear sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onPacketExchangerWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onPacketExchangerWriteException sessionId:{} exception:{}", session.sessionId(),
          e);
    }
  }

  @Override
  public void onPacketExchangerException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onPacketExchangerException sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onPacketExchangerClear(Session session) {
    if (onClear) {
      LOGGER.info("onPacketExchangerClear sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onMycatHandlerWriteException(Session session, Exception e) {
    if (onException) {
      LOGGER.info("onMycatHandlerWriteException sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onMycatHandlerClear(Session session) {
    if (onClear) {
      LOGGER.info("onMycatHandlerClear sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onCloseMysqlSession(MySQLClientSession session, boolean normal, String reason) {
    if (onSessionPool) {
      LOGGER.info("onCloseMysqlSession sessionId:{} normal:{} message:{}", session.sessionId(),
          normal, reason);
    }
  }


  @Override
  public void onRequestException(MySQLClientSession session, Exception e) {
    if (onException) {
      LOGGER.info("onRequestException sessionId:{} exception:{}", session.sessionId(), e);
    }
  }

  @Override
  public void onRequestClear(MySQLClientSession session) {
    if (onClear) {
      LOGGER.info("onRequestClear sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onBackendConCreateException(Session session, Exception e) {
    if (onException && session != null) {
      LOGGER.info("onBackendConCreateException sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onResultSetException(MySQLClientSession session, Exception e) {
    if (onException) {
      LOGGER.info("onResultSetException sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onMycatHandlerReadException(MycatSession mycat, Exception e) {
    if (onException) {
      LOGGER.info("onMycatHandlerReadException sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onMycatHandlerException(MycatSession mycat, Exception e) {
    if (onException) {
      LOGGER.info("onMycatHandlerException sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onMycatHandlerCloseException(MycatSession mycat, ClosedChannelException e) {
    if (onException) {
      LOGGER.info("onMycatHandlerCloseException sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onMycatServerWriteException(MycatSession session, Exception e) {
    if (onException) {
      LOGGER.info("onMycatServerWriteException sessionId:{}", session.sessionId());
    }
  }

  @Override
  public void onGettingBackend(Session session, String replicaName, String defaultDataBase, Exception e) {

  }
//
//  @Override
//  public void onGettingBackend(Session session, String dataNode, Exception e) {
//    if (onException) {
//      LOGGER.info("onGettingBackend sessionId:{}", session.sessionId());
//    }
//  }

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
    if (true) {
      LOGGER.debug("onBackendRead sessionId:{}",session.sessionId());
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
      LOGGER.debug("onAllocateByteBuffer session id{}  {}", session.sessionId(), buffer);
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
//      //    Thread.dumpStack();
//      MySQLSynContextImpl c = new MySQLSynContextImpl(session);
//      LOGGER.debug(
//          "sessionId:{} dataNode:{} isolation: {} charset:{} automCommit:{} characterSetResult:{} sqlSelectLimit:{} netWriteTimeout:{}",
//          session.sessionId(), c.getDefaultDatabase() != null ? c.getDefaultDatabase() : null,
//          c.getIsolation(), c.getCharset(), c.isAutocommit(),
//          c.getCharacterSetResult(), c.getSqlSelectLimit(), c.getNetWriteTimeout());
    }
  }

  @Override
  public final void onRecycleByteBuffer(ByteBuffer buffer, Session session) {
    if (onBuffer) {
      LOGGER.debug("onRecycleByteBuffer sessionId:{}  {}",
          MycatMonitorCallback.getSession().sessionId(), buffer);
    }
  }

  @Override
  public final void onExpandByteBuffer(ByteBuffer buffer, Session session) {
    if (onBuffer) {
      LOGGER.debug("onExpandByteBuffer sessionId:{}  {}",
          MycatMonitorCallback.getSession().sessionId(), buffer);
    }
  }

  @Override
  public final void onNewMycatSession(MycatSession session) {
    if (onSessionPool) {
      LOGGER.debug("onNewMycatSession sessionId:{}", session.sessionId());
    }
  }

  @Override
  public final void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (onBind) {
      LOGGER.debug("onBindMySQLSession sessionId:{} sessionId:{}", mycat.sessionId(),
          session.sessionId());
    }
  }

  @Override
  public final void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
    if (onBind) {
      LOGGER.debug("onUnBindMySQLSession sessionId:{} sessionId:{}", mycat.sessionId(),
          session.sessionId());
    }
  }

  @Override
  public final void onCloseMycatSession(MycatSession session, boolean normal, String reason) {
    if (onSessionPool) {
      LOGGER.debug("onCloseMycatSession sessionId:{} normal:{} message:{}", session.sessionId(),
          normal, reason);
    }
  }

  @Override
  public final void onNewMySQLSession(MySQLClientSession session) {
    if (onSessionPool) {
      LOGGER.debug("onNewMySQLSession sessionId:{} dataSourceName:{}", session.sessionId(),
          session.getDatasource().getName());
    }
  }

  @Override
  public final void onAddIdleMysqlSession(MySQLClientSession session) {
    if (onSessionPool) {
      LOGGER.debug("onAddIdleMysqlSession sessionId:{} dataSourceName:{}", session.sessionId(),
          session.getDatasource().getName());
    }
  }

  @Override
  public final void onGetIdleMysqlSession(MySQLClientSession session) {
    if (onSessionPool) {
      LOGGER.debug("onGetIdleMysqlSession sessionId:{} dataSourceName:{}", session.sessionId(),
          session.getDatasource().getName());
    }
  }

  //todo
  @Override
  public void onRouteSQL(Session session, String replicaName, String defaultDataBase, String sql) {

  }

  @Override
  public void onCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSleepCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onSleepCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSleepCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onSleepCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onQuitCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onQuitCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onQuitCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onQuitCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onQueryCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onQueryCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onQueryCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onQueryCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onInitDbCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onInitDbCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onInitDbCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onInitDbCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onPingCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onPingCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onPingCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onPingCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onFieldListCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onFieldListCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onFieldListCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onFieldListCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSetOptionCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onSetOptionCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onSetOptionCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onSetOptionCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onPrepareCommandStart(MycatSession mycat) {

  }

  @Override
  public void onPrepareCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onPrepareCommandEnd sessionId:{}", mycat.sessionId());
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
      LOGGER.debug("onExecuteCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onExecuteCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onExecuteCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onCloseCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onCloseCommandStart sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onCloseCommandEnd(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onCloseCommandEnd sessionId:{}", mycat.sessionId());
    }
  }

  @Override
  public void onResetCommandStart(MycatSession mycat) {
    if (onCommand) {
      LOGGER.debug("onResetCommandStart sessionId:{}", mycat.sessionId());
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

  @Override
  public void onResultSetEnd(MySQLClientSession mysql) {
    if (recordDump){
      LOGGER.debug("sessionId:{} onResultOk", mysql.sessionId());
    }
  }

  @Override
  public void onRouteSQL(MycatSession mycat, String dataSourceName, String sql) {

  }
}

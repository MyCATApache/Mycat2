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
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public abstract class AbstractMonitorCallback implements MycatMonitorCallback{

  @Override
  public void onFetchCommandStart(MycatSession mycat) {

  }

  @Override
  public void onFetchCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onLoadDataLocalInFileEmptyPacketStart(MycatSession mycat) {

  }

  @Override
  public void onLoadDataLocalInFileEmptyPacketEnd(MycatSession mycat) {

  }

  @Override
  public void onLoadDataLocalInFileContextStart(MycatSession mycat) {

  }

  @Override
  public void onLoadDataLocalInFileContextEnd(MycatSession mycat) {

  }

  @Override
  public void onPacketExchangerRead(Session session) {

  }

  @Override
  public void onPacketExchangerWrite(Session session) {

  }

  @Override
  public void onShutdownCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onStatisticsCommandStart(MycatSession mycat) {

  }

  @Override
  public void onDropDbCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onRefreshCommandStart(MycatSession mycat) {

  }

  @Override
  public void onFieldListCommandStart(MycatSession mycat) {

  }

  @Override
  public void onFieldListCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onMycatServerWriteException(MycatSession session, Exception e) {

  }

  @Override
  public void onGettingBackend(Session session, String replicaName, String defaultDataBase, Exception e) {

  }

  @Override
  public void onMycatHandlerCloseException(MycatSession mycat, ClosedChannelException e) {

  }

  @Override
  public void onMycatHandlerException(MycatSession mycat, Exception e) {

  }

  @Override
  public void onMycatHandlerReadException(MycatSession mycat, Exception e) {

  }

  @Override
  public void onBackendConCreateException(Session session, Exception e) {

  }

  @Override
  public void onResultSetException(MySQLClientSession session, Exception e) {

  }

  @Override
  public void onRequestClear(MySQLClientSession session) {

  }

  @Override
  public void onRequestException(MySQLClientSession session, Exception e) {

  }

  @Override
  public void onBackendConCreateConnectException(Session session, Exception e) {

  }

  @Override
  public void onIdleReadException(Session session, Exception e) {

  }

  public void onOrginSQL(Session session, String sql) {

  }


  public void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {

  }


  public void onPayloadType(Session session, MySQLPayloadType type) {

  }



  public void onBackendWrite(Session session, ByteBuffer view, int startIndex, int len) {

  }


  public void onBackendRead(Session session, ByteBuffer view, int startIndex, int len) {

  }


  public void onFrontWrite(Session session, ByteBuffer view, int startIndex, int len) {

  }


  public void onMySQLSessionServerStatusChanged(Session session, int serverStatus) {

  }


  public void onAllocateByteBuffer(ByteBuffer buffer, Session session) {

  }


  public void onSynchronizationState(MySQLClientSession session) {

  }



  public void onRecycleByteBuffer(ByteBuffer buffer, Session session) {

  }


  public void onExpandByteBuffer(ByteBuffer buffer, Session session) {

  }


  public void onNewMycatSession(MycatSession session) {

  }


  public void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {

  }


  public void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {

  }


  public void onNewMySQLSession(MySQLClientSession session) {

  }


  public void onAddIdleMysqlSession(MySQLClientSession session) {

  }


  public void onGetIdleMysqlSession(MySQLClientSession session) {

  }

  @Override
  public void onRouteSQL(Session session, String replicaName, String defaultDataBase, String sql) {

  }


  public void onCloseMysqlSession(MySQLClientSession session,boolean noraml,String reson) {

  }


  public void onRouteSQLResult(Session session, String dataNodeName, String defaultDataBase,
      String dataNode,
      byte[] payload) {

  }


  @Override
  public void onBackendConCreateClear(Session session) {

  }





  @Override
  public void onAuthHandlerClear(Session session) {

  }



  @Override
  public void onPacketExchangerClear(Session session) {

  }



  @Override
  public void onMycatHandlerClear(Session session) {

  }

  @Override
  public void onBackendConCreateWriteException(Session session, Exception e) {

  }

  @Override
  public void onBackendConCreateReadException(Session session, Exception e) {

  }

  @Override
  public void onBackendResultSetReadException(Session session, Exception e) {

  }

  @Override
  public void onBackendResultSetWriteException(Session session, Exception e) {

  }

  @Override
  public void onBackendResultSetWriteClear(Session session) {

  }

  @Override
  public void onResultSetWriteException(Session session, Exception e) {

  }

  @Override
  public void onResultSetReadException(Session session, Exception e) {

  }

  @Override
  public void onResultSetClear(Session session) {

  }

  @Override
  public void onAuthHandlerWriteException(Session session, Exception e) {

  }

  @Override
  public void onAuthHandlerReadException(Session session, Exception e) {

  }

  @Override
  public void onPacketExchangerWriteException(Session session, Exception e) {

  }

  @Override
  public void onPacketExchangerException(Session session, Exception e) {

  }

  @Override
  public void onMycatHandlerWriteException(Session session, Exception e) {

  }

  @Override
  public void onCloseMycatSession(MycatSession mycat, boolean normal, String reason) {

  }

  @Override
  public void onCommandStart(MycatSession mycat) {

  }

  @Override
  public void onCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onSleepCommandStart(MycatSession mycat) {

  }

  @Override
  public void onSleepCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onQuitCommandStart(MycatSession mycat) {

  }

  @Override
  public void onQuitCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onQueryCommandStart(MycatSession mycat) {

  }

  @Override
  public void onQueryCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onInitDbCommandStart(MycatSession mycat) {

  }

  @Override
  public void onInitDbCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onPingCommandStart(MycatSession mycat) {

  }

  @Override
  public void onPingCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onSetOptionCommandStart(MycatSession mycat) {

  }

  @Override
  public void onSetOptionCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onPrepareCommandStart(MycatSession mycat) {

  }

  @Override
  public void onPrepareCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onSendLongDataCommandStart(MycatSession mycat) {

  }

  @Override
  public void onSendLongDataCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onExecuteCommandStart(MycatSession mycat) {

  }

  @Override
  public void onExecuteCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onCloseCommandStart(MycatSession mycat) {

  }

  @Override
  public void onCloseCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onResetCommandStart(MycatSession mycat) {

  }

  @Override
  public void onResetCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onCreateDbCommandStart(MycatSession mycat) {

  }

  @Override
  public void onCreateDbCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onDropDbCommandStart(MycatSession mycat) {

  }

  @Override
  public void onRefreshCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onShutdownCommandStart(MycatSession mycat) {

  }

  @Override
  public void onStatisticsCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onProcessInfoCommandStart(MycatSession mycat) {

  }

  @Override
  public void onProcessInfoCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onConnectCommandStart(MycatSession mycat) {

  }

  @Override
  public void onConnectCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onProcessKillCommandStart(MycatSession mycat) {

  }

  @Override
  public void onProcessKillCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onDebugCommandStart(MycatSession mycat) {

  }

  @Override
  public void onDebugCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onTimeCommandStart(MycatSession mycat) {

  }

  @Override
  public void onTimeCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onDelayedInsertCommandStart(MycatSession mycat) {

  }

  @Override
  public void onDelayedInsertCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onChangeUserCommandStart(MycatSession mycat) {

  }

  @Override
  public void onChangeUserCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onResetConnectionCommandStart(MycatSession mycat) {

  }

  @Override
  public void onResetConnectionCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onDaemonCommandStart(MycatSession mycat) {

  }

  @Override
  public void onDaemonCommandEnd(MycatSession mycat) {

  }

  @Override
  public void onChange2ReadOpts(Session session) {

  }

  @Override
  public void onChange2WriteOpts(Session session) {

  }

  @Override
  public void onClearReadWriteOpts(Session session) {

  }

  @Override
  public void onSyncSQL(MySQLSynContext mycatSession, String sql, MySQLClientSession session) {

  }

  @Override
  public void onResultSetEnd(MySQLClientSession mysql) {

  }

  @Override
  public void onRouteSQL(MycatSession mycat, String dataSourceName, String sql) {

  }


  @Override
  public void onRouteSQLResult(Session session, String dataNodeName, String defaultDataBase,
      String dataSource, String sql) {

  }
}
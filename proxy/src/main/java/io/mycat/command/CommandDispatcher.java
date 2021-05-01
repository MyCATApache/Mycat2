/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.command;

import io.mycat.proxy.session.MySQLServerSession;
import io.mycat.proxy.session.MycatSession;
import io.vertx.core.Future;

import java.util.Map;

/**
 * @author jamie12221 date 2019-05-09 02:30
 *
 * command 报文解析分发
 **/
public interface CommandDispatcher extends LocalInFileRequestParseHelper,
    PrepareStatementParserHelper {
  void initRuntime(MySQLServerSession session);
  Future<Void> handleQuery(byte[] sql, MySQLServerSession session);

  Future<Void>  handleSleep(MySQLServerSession session);

  Future<Void>  handleQuit(MySQLServerSession session);

  Future<Void>  handleInitDb(String db, MySQLServerSession session);

  Future<Void>  handlePing(MySQLServerSession session);

  Future<Void>  handleFieldList(String table, String filedWildcard, MySQLServerSession session);

  Future<Void>  handleSetOption(boolean on, MySQLServerSession session);

  Future<Void>  handleCreateDb(String schemaName, MySQLServerSession session);

  Future<Void>  handleDropDb(String schemaName, MySQLServerSession session);

  Future<Void>  handleRefresh(int subCommand, MySQLServerSession session);

  Future<Void>  handleShutdown(int shutdownType, MySQLServerSession session);

  Future<Void>  handleStatistics(MySQLServerSession session);

  Future<Void>  handleProcessInfo(MySQLServerSession session);

  Future<Void>  handleConnect(MySQLServerSession session);

  Future<Void>  handleProcessKill(long connectionId, MySQLServerSession session);

  Future<Void>  handleDebug(MySQLServerSession session);

  Future<Void>  handleTime(MySQLServerSession session);

  Future<Void>  handleChangeUser(String userName, String authResponse, String schemaName,
      int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
                                 MySQLServerSession session);

  Future<Void>  handleDelayedInsert(MySQLServerSession session);

  Future<Void>  handleResetConnection(MySQLServerSession session);

  Future<Void>  handleDaemon(MySQLServerSession session);

}

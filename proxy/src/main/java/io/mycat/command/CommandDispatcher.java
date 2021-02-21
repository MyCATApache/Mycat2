/**
 * Copyright (C) <2019>  <chen junwen>
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
  void initRuntime(MycatSession session);
  Future<Void> handleQuery(byte[] sql, MycatSession session);

  Future<Void>  handleSleep(MycatSession session);

  Future<Void>  handleQuit(MycatSession session);

  Future<Void>  handleInitDb(String db, MycatSession session);

  Future<Void>  handlePing(MycatSession session);

  Future<Void>  handleFieldList(String table, String filedWildcard, MycatSession session);

  Future<Void>  handleSetOption(boolean on, MycatSession session);

  Future<Void>  handleCreateDb(String schemaName, MycatSession session);

  Future<Void>  handleDropDb(String schemaName, MycatSession session);

  Future<Void>  handleRefresh(int subCommand, MycatSession session);

  Future<Void>  handleShutdown(int shutdownType, MycatSession session);

  Future<Void>  handleStatistics(MycatSession session);

  Future<Void>  handleProcessInfo(MycatSession session);

  Future<Void>  handleConnect(MycatSession session);

  Future<Void>  handleProcessKill(long connectionId, MycatSession session);

  Future<Void>  handleDebug(MycatSession session);

  Future<Void>  handleTime(MycatSession session);

  Future<Void>  handleChangeUser(String userName, String authResponse, String schemaName,
      int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
      MycatSession session);

  Future<Void>  handleDelayedInsert(MycatSession session);

  Future<Void>  handleResetConnection(MycatSession session);

  Future<Void>  handleDaemon(MycatSession session);

}

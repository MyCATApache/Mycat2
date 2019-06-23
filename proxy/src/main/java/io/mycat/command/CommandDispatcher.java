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

import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;
import java.util.Map;

/**
 * @author jamie12221 date 2019-05-09 02:30
 *
 * command 报文解析分发
 **/
public interface CommandDispatcher extends LocalInFileRequestParseHelper,
    PrepareStatementParserHelper {
  void initRuntime(MycatSession session,ProxyRuntime runtime);
  void handleQuery(byte[] sql, MycatSession session);

  void handleSleep(MycatSession session);

  void handleQuit(MycatSession session);

  void handleInitDb(String db, MycatSession session);

  void handlePing(MycatSession session);

  void handleFieldList(String table, String filedWildcard, MycatSession session);

  void handleSetOption(boolean on, MycatSession session);

  void handleCreateDb(String schemaName, MycatSession session);

  void handleDropDb(String schemaName, MycatSession session);

  void handleRefresh(int subCommand, MycatSession session);

  void handleShutdown(int shutdownType, MycatSession session);

  void handleStatistics(MycatSession session);

  void handleProcessInfo(MycatSession session);

  void handleConnect(MycatSession session);

  void handleProcessKill(long connectionId, MycatSession session);

  void handleDebug(MycatSession session);

  void handleTime(MycatSession session);

  void handleChangeUser(String userName, String authResponse, String schemaName,
      int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
      MycatSession session);

  void handleDelayedInsert(MycatSession session);

  void handleResetConnection(MycatSession session);

  void handleDaemon(MycatSession session);

  abstract class AbstractCommandHandler implements CommandDispatcher {


    public void handleSleep(MycatSession session) {
      session.writeErrorEndPacket();
    }


    public void handleRefresh(int subCommand, MycatSession session) {
      session.writeErrorEndPacket();
    }


    public void handleShutdown(int shutdownType, MycatSession session) {
      session.writeErrorEndPacket();
    }


    public void handleConnect(MycatSession session) {
      session.writeErrorEndPacket();
    }


    public void handleDebug(MycatSession session) {
      session.writeErrorEndPacket();
    }


    public void handleTime(MycatSession session) {
      session.writeErrorEndPacket();
    }


    public void handleDelayedInsert(MycatSession session) {
      session.writeErrorEndPacket();
    }


    public void handleDaemon(MycatSession session) {
      session.writeErrorEndPacket();
    }


  }
}

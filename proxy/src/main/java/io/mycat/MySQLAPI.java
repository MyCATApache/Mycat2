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
package io.mycat;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLPrepareStmtExecuteFlag;
import io.mycat.beans.mysql.MySQLPreparedStatement;
import io.mycat.beans.mysql.MySQLSetOption;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.client.QueryUtil;
import io.mycat.proxy.task.client.prepareStatement.ExecuteTask;
import io.mycat.proxy.task.client.prepareStatement.PrepareStmtUtil;
import io.mycat.proxy.task.client.prepareStatement.PrepareTask;
import io.mycat.proxy.task.client.prepareStatement.SendLongDataTask;
import io.mycat.proxy.task.client.resultset.ResultSetCollector;

public interface MySQLAPI {

  MySQLClientSession getThis();

  default void commit(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), 3, "commit;", callback);
  }


  default void execute(MySQLPreparedStatement preparedStatement, MySQLPrepareStmtExecuteFlag flags,
      ResultSetCollector collector, AsyncTaskCallBack<MySQLClientSession> callBack) {
    new ExecuteTask().request(getThis(), preparedStatement, flags, collector, callBack);
  }


  default void prepare(String prepareStatement, AsyncTaskCallBack<MySQLClientSession> callback) {
    new PrepareTask().request(getThis(), prepareStatement, callback);
  }

  default void sleep(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_SLEEP, new byte[]{}, callback);
  }

  default void quit(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_QUIT, new byte[]{}, callback);
  }

  default void fieldList(String tableName, AsyncTaskCallBack<MySQLClientSession> callback) {
    tableName = tableName + '\0';
    byte[] bytes = tableName.getBytes();
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_FIELD_LIST, bytes, callback);
  }

  default void createDB(String schemaName, AsyncTaskCallBack<MySQLClientSession> callback) {
    byte[] bytes = schemaName.getBytes();
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_CREATE_DB, bytes, callback);
  }

  default void dropDB(String schemaName, AsyncTaskCallBack<MySQLClientSession> callback) {
    byte[] bytes = schemaName.getBytes();
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_DROP_DB, bytes, callback);
  }

  default void refresh(byte subCommand, AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND
        .request(getThis(), MySQLCommandType.COM_REFRESH, new byte[]{subCommand}, callback);
  }

  default void shutdown(byte shutdownType, AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND
        .request(getThis(), MySQLCommandType.COM_SHUTDOWN, new byte[]{shutdownType}, callback);
  }

  default void statistics(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_STATISTICS, new byte[]{}, callback);
  }

  default void processInfo(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_PROCESS_INFO, new byte[]{}, callback);
  }

  default void connect(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_CONNECT, new byte[]{}, callback);
  }

  default void processKill(long connectionId, AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_PROCESS_KILL, connectionId, callback);
  }

  default void debug(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_DEBUG, new byte[]{}, callback);
  }

  default void ping(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_PING, new byte[]{}, callback);
  }


  default void time(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_TIME, new byte[]{}, callback);
  }

  default void delayedInsert(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND
        .request(getThis(), MySQLCommandType.COM_DELAYED_INSERT, new byte[]{}, callback);
  }

  default void resetConnection(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND
        .request(getThis(), MySQLCommandType.COM_RESET_CONNECTION, new byte[]{}, callback);
  }

  default void changeUser(AsyncTaskCallBack<MySQLClientSession> callback) {
    throw new MycatExpection("unsupport!");
  }

  default void daemon(AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), MySQLCommandType.COM_DAEMON, new byte[]{}, callback);
  }

  default void sendBlob(MySQLPreparedStatement preparedStatement, int index, byte[] data,
      AsyncTaskCallBack<MySQLClientSession> callback) {
    preparedStatement.put(index, data);
    new SendLongDataTask().request(getThis(), preparedStatement, callback);
  }


  default void reset(MySQLPreparedStatement preparedStatement,
      AsyncTaskCallBack<MySQLClientSession> callback) {
    preparedStatement.resetLongData();
    PrepareStmtUtil.reset(getThis(), preparedStatement.getStatementId(), callback);
  }

  default void setOption(MySQLSetOption option, AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.setOption(getThis(), option, callback);
  }

  default void fetch(long stmtId, long numRows, AsyncTaskCallBack<MySQLClientSession> callback) {
    PrepareStmtUtil.fetch(getThis(), stmtId, numRows, callback);
  }


  default void doQuery(String sql, AsyncTaskCallBack<MySQLClientSession> callbac) {
    QueryUtil.COMMAND.request(getThis(), 3, sql, callbac);
  }


  default void initDb(String dataBase, AsyncTaskCallBack<MySQLClientSession> callback) {
    QueryUtil.COMMAND.request(getThis(), 2, dataBase, callback);
  }


  default void close(MySQLPreparedStatement preparedStatement,
      AsyncTaskCallBack<MySQLClientSession> callback) {
    preparedStatement.resetLongData();
    PrepareStmtUtil.close(getThis(), preparedStatement.getStatementId(), callback);
  }
}

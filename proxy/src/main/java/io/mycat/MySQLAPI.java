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
import io.mycat.MycatExpection;
import io.mycat.proxy.packet.ResultSetCollector;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.AsynTaskFuture;
import io.mycat.proxy.task.CommandTask;
import io.mycat.proxy.task.DescTask;
import io.mycat.proxy.task.LoadDataRequestTask;
import io.mycat.proxy.task.MappedByteBufferPayloadWriter;
import io.mycat.proxy.task.ShowTablesTask;
import io.mycat.proxy.task.prepareStatement.CloseTask;
import io.mycat.proxy.task.prepareStatement.ExecuteTask;
import io.mycat.proxy.task.prepareStatement.FetchTask;
import io.mycat.proxy.task.prepareStatement.PrepareTask;
import io.mycat.proxy.task.prepareStatement.ResetTask;
import io.mycat.proxy.task.prepareStatement.SendLongDataTask;
import io.mycat.proxy.task.prepareStatement.SetOptionTask;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

public interface MySQLAPI {

  MySQLClientSession getThis();

  default void commit(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), 3, "commit;", callback);
  }

  default AsynTaskFuture<MySQLClientSession> commit() {
    AsynTaskFuture<MySQLClientSession> future = AsynTaskFuture.future();
    commit(future);
    return future;
  }

  default void execute(MySQLPreparedStatement preparedStatement, MySQLPrepareStmtExecuteFlag flags,
      ResultSetCollector collector, AsynTaskCallBack<MySQLClientSession> callBack) {
    new ExecuteTask().request(getThis(), preparedStatement, flags, collector, callBack);
  }

  default AsynTaskFuture<MySQLClientSession> execute(MySQLPreparedStatement preparedStatement,
      MySQLPrepareStmtExecuteFlag flags, ResultSetCollector collector) {
    AsynTaskFuture<MySQLClientSession> future = AsynTaskFuture.future();
    execute(preparedStatement, flags, collector, future);
    return future;
  }

  default AsynTaskFuture<MySQLClientSession> prepare(String prepareStatement) {
    AsynTaskFuture<MySQLClientSession> future = AsynTaskFuture.future();
    prepare(prepareStatement, future);
    return future;
  }

  default void prepare(String prepareStatement, AsynTaskCallBack<MySQLClientSession> callback) {
    new PrepareTask().request(getThis(), prepareStatement, callback);
  }

  default AsynTaskFuture<MySQLClientSession> sendBlob(MySQLPreparedStatement preparedStatement, int index,
      byte[] data) {
    AsynTaskFuture<MySQLClientSession> future = AsynTaskFuture.future();
    sendBlob(preparedStatement, index, data, future);
    return future;
  }

  default void sleep(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_SLEEP, new byte[]{}, callback);
  }

  default void quit(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_QUIT, new byte[]{}, callback);
  }

  default void fieldList(String tableName, AsynTaskCallBack<MySQLClientSession> callback) {
    tableName = tableName + '\0';
    byte[] bytes = tableName.getBytes();
    new CommandTask().request(getThis(), MySQLCommandType.COM_FIELD_LIST, bytes, callback);
  }

  default void createDB(String schemaName, AsynTaskCallBack<MySQLClientSession> callback) {
    byte[] bytes = schemaName.getBytes();
    new CommandTask().request(getThis(), MySQLCommandType.COM_CREATE_DB, bytes, callback);
  }

  default void dropDB(String schemaName, AsynTaskCallBack<MySQLClientSession> callback) {
    byte[] bytes = schemaName.getBytes();
    new CommandTask().request(getThis(), MySQLCommandType.COM_DROP_DB, bytes, callback);
  }

  default void refresh(byte subCommand, AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask()
        .request(getThis(), MySQLCommandType.COM_REFRESH, new byte[]{subCommand}, callback);
  }

  default void shutdown(byte shutdownType, AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask()
        .request(getThis(), MySQLCommandType.COM_SHUTDOWN, new byte[]{shutdownType}, callback);
  }

  default void statistics(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_STATISTICS, new byte[]{}, callback);
  }

  default void processInfo(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_PROCESS_INFO, new byte[]{}, callback);
  }

  default void connect(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_CONNECT, new byte[]{}, callback);
  }

  default void processKill(long connectionId, AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_PROCESS_KILL, connectionId, callback);
  }

  default void debug(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_DEBUG, new byte[]{}, callback);
  }

  default void ping(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_PING, new byte[]{}, callback);
  }


  default void time(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_TIME, new byte[]{}, callback);
  }

  default void delayedInsert(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask()
        .request(getThis(), MySQLCommandType.COM_DELAYED_INSERT, new byte[]{}, callback);
  }

  default void resetConnection(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask()
        .request(getThis(), MySQLCommandType.COM_RESET_CONNECTION, new byte[]{}, callback);
  }

  default void changeUser(AsynTaskCallBack<MySQLClientSession> callback) {
    throw new MycatExpection("unsupport!");
  }

  default void daemon(AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), MySQLCommandType.COM_DAEMON, new byte[]{}, callback);
  }

  default void sendBlob(MySQLPreparedStatement preparedStatement, int index, byte[] data,
      AsynTaskCallBack<MySQLClientSession> callback) {
    preparedStatement.put(index, data);
    new SendLongDataTask().request(getThis(), preparedStatement, callback);
  }

  default AsynTaskFuture<MySQLClientSession> reset(MySQLPreparedStatement preparedStatement) {
    AsynTaskFuture<MySQLClientSession> future = AsynTaskFuture.future();
    reset(preparedStatement, future);
    return future;
  }

  default void reset(MySQLPreparedStatement preparedStatement, AsynTaskCallBack<MySQLClientSession> callback) {
    preparedStatement.resetLongData();
    new ResetTask().request(getThis(), 0x1a, preparedStatement.getStatementId(), callback);
  }

  default void setOption(MySQLSetOption option, AsynTaskCallBack<MySQLClientSession> callback) {
    new SetOptionTask().request(getThis(), option, callback);
  }

  default void fetch(long stmtId, long numRows, AsynTaskCallBack<MySQLClientSession> callback) {
    new FetchTask().request(getThis(),stmtId, numRows, callback);
  }

  default AsynTaskFuture<MySQLClientSession> close(MySQLPreparedStatement preparedStatement) {
    AsynTaskFuture<MySQLClientSession> future = AsynTaskFuture.future();
    preparedStatement.resetLongData();
    close(preparedStatement, future);
    return future;
  }

  default void doQuery(String sql, AsynTaskCallBack<MySQLClientSession> callbac) {
    new CommandTask().request(getThis(), 3, sql, callbac);
  }

  default void showTables(AsynTaskCallBack<MySQLClientSession> callbac) {
    new ShowTablesTask().request(getThis(), 3, "show tables;", callbac);
  }

  default void desc(String tableName, AsynTaskCallBack<MySQLClientSession> callbac) {
    new DescTask().request(getThis(), 3, "select * from " + tableName + " limit 0;",
        new AsynTaskCallBack<MySQLClientSession>() {
          @Override
          public void finished(MySQLClientSession session, Object sender, boolean success, Object result,
              Object attr) {
            new DescTask()
                .request(getThis(), 3, "select * from " + tableName + " limit 0;", callbac);
          }
        });
  }

  default AsynTaskFuture<MySQLClientSession> doQuery(String sql) {
    AsynTaskFuture<MySQLClientSession> future = AsynTaskFuture.future();
    doQuery(sql, future);
    return future;
  }

  default void initDb(String dataBase, AsynTaskCallBack<MySQLClientSession> callback) {
    new CommandTask().request(getThis(), 2, dataBase, callback);
  }

  default void loadData(String sql, AsynTaskCallBack<MySQLClientSession> callback) {
    new LoadDataRequestTask().request(getThis(), 3, sql, new AsynTaskCallBack<MySQLClientSession>() {
      @Override
      public void finished(MySQLClientSession session, Object sender, boolean success, Object result,
          Object attr) {
        try {
          FileChannel open = FileChannel.open(Paths.get((String) result));
          loadDataFileContext(open, 0, (int) open.size(), new AsynTaskCallBack<MySQLClientSession>() {
            @Override
            public void finished(MySQLClientSession session, Object sender, boolean success,
                Object packetId, Object attr) {
              if (success) {
                session.loadDataEmptyPacket(callback, session.incrementPacketIdAndGet());
              } else {
                callback.finished(session, this, false, null, attr);
              }
            }
          });
        } catch (Exception e) {

          callback.finished(session, this, false, null, attr);
        }
      }
    });

  }

  default void loadDataEmptyPacket(AsynTaskCallBack<MySQLClientSession> callback, byte nextPacketId) {
    new CommandTask().requestEmptyPacket(getThis(), nextPacketId, callback);
  }

  default void loadDataFileContext(FileChannel fileChannel, int position, int length,
      AsynTaskCallBack<MySQLClientSession> callback) throws Exception {
    new MappedByteBufferPayloadWriter()
        .request(getThis(), fileChannel.map(FileChannel.MapMode.READ_ONLY, position, length),
            position, length, callback);
  }

  default void close(MySQLPreparedStatement preparedStatement, AsynTaskCallBack<MySQLClientSession> callback) {
    preparedStatement.resetLongData();
    new CloseTask().request(getThis(), preparedStatement.getStatementId(), callback);
  }
}

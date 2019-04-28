package io.mycat.proxy.task;

import io.mycat.beans.mysql.PrepareStmtExecuteFlag;
import io.mycat.proxy.packet.ResultSetCollector;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.task.prepareStatement.*;

import java.nio.channels.FileChannel;
import java.nio.file.Paths;

public interface MySQLAPI {
    MySQLSession getThis();

    default void commit(AsynTaskCallBack<MySQLSession> callback) {
        new CommandTask().request(getThis(), 3, "commit;", callback);
    }

    default AsynTaskFuture<MySQLSession> commit() {
        AsynTaskFuture<MySQLSession> future = AsynTaskFuture.future();
        commit(future);
        return future;
    }

    default void execute(PreparedStatement preparedStatement, PrepareStmtExecuteFlag flags, ResultSetCollector collector, AsynTaskCallBack<MySQLSession> callBack) {
        new ExecuteTask().request(getThis(), preparedStatement, flags, collector, callBack);
    }

    default AsynTaskFuture<MySQLSession> execute(PreparedStatement preparedStatement, PrepareStmtExecuteFlag flags, ResultSetCollector collector) {
        AsynTaskFuture<MySQLSession> future = AsynTaskFuture.future();
        execute(preparedStatement, flags, collector, future);
        return future;
    }

    default AsynTaskFuture<MySQLSession> prepare(String prepareStatement) {
        AsynTaskFuture<MySQLSession> future = AsynTaskFuture.future();
        prepare(prepareStatement, future);
        return future;
    }

    default void prepare(String prepareStatement, AsynTaskCallBack<MySQLSession> callback) {
        new PrepareTask().request(getThis(), prepareStatement, callback);
    }

    default AsynTaskFuture<MySQLSession> sendBlob(PreparedStatement preparedStatement, int index, byte[] data) {
        AsynTaskFuture<MySQLSession> future = AsynTaskFuture.future();
        sendBlob(preparedStatement, index, data, future);
        return future;
    }

    default void sendBlob(PreparedStatement preparedStatement, int index, byte[] data, AsynTaskCallBack<MySQLSession> callback) {
        preparedStatement.put(index, data);
        new SendLongDataTask().request(getThis(), preparedStatement, callback);
    }

    default AsynTaskFuture<MySQLSession> reset(PreparedStatement preparedStatement) {
        AsynTaskFuture<MySQLSession> future = AsynTaskFuture.future();
        reset(preparedStatement, future);
        return future;
    }

    default void reset(PreparedStatement preparedStatement, AsynTaskCallBack<MySQLSession> callback) {
        preparedStatement.resetLongData();
        new ResetTask().request(getThis(), 0x1a, preparedStatement.getStatementId(), callback);
    }

    default AsynTaskFuture<MySQLSession> close(PreparedStatement preparedStatement) {
        AsynTaskFuture<MySQLSession> future = AsynTaskFuture.future();
        preparedStatement.resetLongData();
        close(preparedStatement, future);
        return future;
    }

    default void doQuery(String sql, AsynTaskCallBack<MySQLSession> callbac) {
        new CommandTask().request(getThis(), 3, sql, callbac);
    }
    default void showTables(AsynTaskCallBack<MySQLSession> callbac) {
        new ShowTablesTask().request(getThis(), 3, "show tables;", callbac);
    }
    default void desc(String tableName,AsynTaskCallBack<MySQLSession> callbac) {
        new DescTask().request(getThis(), 3, "select * from " + tableName + " limit 0;", new AsynTaskCallBack<MySQLSession>() {
            @Override
            public void finished(MySQLSession session, Object sender, boolean success, Object result, Object attr) {
                new DescTask().request(getThis(), 3, "select * from "+tableName+" limit 0;", callbac);
            }
        });
    }

    default AsynTaskFuture<MySQLSession> doQuery(String sql) {
        AsynTaskFuture<MySQLSession> future = AsynTaskFuture.future();
        doQuery(sql, future);
        return future;
    }

    default void initDb(String dataBase, AsynTaskCallBack<MySQLSession> callback) {
        new CommandTask().request(getThis(), 2, dataBase, callback);
    }

    default void loadData(String sql, AsynTaskCallBack<MySQLSession> callback) {
        new LoadDataRequestTask().request(getThis(), 3, sql, new AsynTaskCallBack<MySQLSession>() {
            @Override
            public void finished(MySQLSession session, Object sender, boolean success, Object result, Object attr) {
                try {
                    FileChannel open = FileChannel.open(Paths.get((String) result));
                    loadDataFileContext(open, 0, (int) open.size(), new AsynTaskCallBack<MySQLSession>() {
                        @Override
                        public void finished(MySQLSession session, Object sender, boolean success, Object packetId, Object attr) {
                            if (success) {
                                session.loadDataEmptyPacket(callback,session.incrementPacketIdAndGet());
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

    default void loadDataEmptyPacket(AsynTaskCallBack<MySQLSession> callback, byte nextPacketId) {
        new CommandTask().requestEmptyPacket(getThis(),nextPacketId, callback);
    }

    default void loadDataFileContext(FileChannel fileChannel, int position, int length, AsynTaskCallBack<MySQLSession> callback)throws  Exception{
        new MappedByteBufferPayloadWriter().request(getThis(), fileChannel.map(FileChannel.MapMode.READ_ONLY,position,length), position, length, callback);
    }

    default void close(PreparedStatement preparedStatement, AsynTaskCallBack<MySQLSession> callback) {
        preparedStatement.resetLongData();
        new CloseTask().request(getThis(), preparedStatement.getStatementId(), callback);
    }
}

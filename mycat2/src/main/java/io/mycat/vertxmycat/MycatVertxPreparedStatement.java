package io.mycat.vertxmycat;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.*;

public class MycatVertxPreparedStatement implements PreparedStatement {
    private final String sql;
    private final AbstractMySqlConnection abstractMySqlConnection;

    public MycatVertxPreparedStatement(String sql,AbstractMySqlConnection abstractMySqlConnection) {
        this.sql = sql;
        this.abstractMySqlConnection = abstractMySqlConnection;
    }

    @Override
    public PreparedQuery<RowSet<Row>> query() {
        return abstractMySqlConnection.preparedQuery(sql);
    }

    @Override
    public Cursor cursor(Tuple args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowStream<Row> createStream(int fetch, Tuple args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }

    @Override
    public void close(Handler<AsyncResult<Void>> completionHandler) {
        Future<Void> close = close();
        if (close!=null){
            close.onComplete(completionHandler);
        }
        completionHandler.handle(Future.succeededFuture());
    }
}

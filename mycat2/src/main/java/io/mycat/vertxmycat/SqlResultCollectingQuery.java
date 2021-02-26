package io.mycat.vertxmycat;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;

import java.util.function.Function;
import java.util.stream.Collector;

import static io.mycat.vertxmycat.RowSetQuery.runTextQuery;

public class SqlResultCollectingQuery<R> implements Query<SqlResult<R>> {
        private final Collector<Row, ?, R> collectorArg;
        private final String sql;
        private final AbstractMySqlConnectionImpl connection;

        public SqlResultCollectingQuery(
                String sql,AbstractMySqlConnectionImpl connection,Collector<Row, ?, R> collectorArg) {
            this.sql = sql;
            this.connection = connection;
            this.collectorArg = collectorArg;
        }

        @Override
        public void execute(Handler<AsyncResult<SqlResult<R>>> handler) {
            Future<SqlResult<R>> future = execute();
            if (future != null) {
                future.onComplete(handler);
            }
        }

        @Override
        public Future<SqlResult<R>> execute() {
           return (Future)runTextQuery(sql, connection.mySQLClientSession, collectorArg).future();
        }

        @Override
        public <R> Query<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U> Query<RowSet<U>> mapping(Function<Row, U> mapper) {
            throw new UnsupportedOperationException();
        }
    }
package io.mycat.vertxmycat;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.*;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.apply;
import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.toObjects;

public class SqlResultCollectingPrepareQuery<R> implements AbstractMySqlPreparedQuery<SqlResult<R>> {

    private final String sql;
    private final AbstractMySqlConnectionImpl connection;
    private final Collector<Row, Object, R> collector;

    public SqlResultCollectingPrepareQuery(String sql,
                                               AbstractMySqlConnectionImpl connection,
                                               Collector<Row, Object, R> collector) {
        this.sql = sql;
        this.connection = connection;
        this.collector = collector;
    }

    @Override
    public Future<SqlResult<R>> execute(Tuple tuple) {
        Query<RowSet<Row>> query = connection.query(apply(sql, toObjects(tuple)));
        return query.collecting(collector).execute();
    }

    @Override
    public Future<SqlResult<R>> executeBatch(List<Tuple> batch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<SqlResult<R>> execute() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> PreparedQuery<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <U> PreparedQuery<RowSet<U>> mapping(Function<Row, U> mapper) {
        throw new UnsupportedOperationException();
    }
}

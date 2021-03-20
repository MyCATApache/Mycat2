package io.mycat.vertxmycat;

import io.vertx.core.Future;
import io.vertx.sqlclient.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

public class SqlResultCollectingPrepareJdbcQuery<R> implements AbstractMySqlPreparedQuery<SqlResult<R>> {

    private final String sql;
    private final Connection connection;
    private final Collector<Row, ?, R> collector;
    private ReadWriteThreadPool threadPool;

    public SqlResultCollectingPrepareJdbcQuery(String sql,
                                               Connection connection,
                                               Collector<Row, ?, R> collector, ReadWriteThreadPool threadPool) {
        this.sql = sql;
        this.connection = connection;
        this.collector = collector;
        this.threadPool = threadPool;
    }

    @Override
    public Future<SqlResult<R>> execute(Tuple tuple) {
        return Future.future(promise -> {
            threadPool.execute(true, () -> {
                try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    if (tuple.size()>0) {
                        RowSetJdbcPreparedJdbcQuery.setParams(tuple, preparedStatement);
                    }
                    preparedStatement.execute();
                    RowSetJdbcPreparedJdbcQuery.extracted(promise, preparedStatement, preparedStatement.getResultSet(), collector);
                } catch (Throwable throwable) {
                    promise.tryFail(throwable);
                }
            });
        });
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

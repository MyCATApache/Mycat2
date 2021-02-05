package io.mycat.vertxmycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import io.vertx.core.VertxException;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.apply;
import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.toObjects;

public class RowSetMySqlPreparedQuery implements AbstractMySqlPreparedQuery<RowSet<Row>> {

    private final String sql;
    private final AbstractMySqlConnectionImpl connection;

    public RowSetMySqlPreparedQuery(String sql, AbstractMySqlConnectionImpl connection) {
        this.sql = sql;
        this.connection = connection;
    }

    @Override
    public Future<RowSet<Row>> execute(Tuple tuple) {
        Query<RowSet<Row>> query = connection.query(apply(sql, toObjects(tuple)));
        return query.execute();
    }

    @Override
    public Future<RowSet<Row>> executeBatch(List<Tuple> batch) {
        Future<Void> future = Future.succeededFuture();
        List<long[]> list = new ArrayList<>();
        for (Tuple tuple : batch) {
            String eachSql = apply(sql, toObjects(tuple));
            future = future.flatMap(unused -> {
                Query<RowSet<Row>> query = connection.query(eachSql);
                return query.execute().map(rows -> {
                    list.add(new long[]{rows.rowCount(), rows.property(MySQLClient.LAST_INSERTED_ID)});
                    return null;
                });
            });
        }
       return future.map(unused -> {
            long[] reduce = list.stream().reduce(new long[]{0, 0}, (longs, longs2) -> new long[]{longs[0] + longs2[0], Math.max(longs[1] ,longs2[1])});
            VertxRowSetImpl vertxRowSet = new VertxRowSetImpl();
            vertxRowSet.setAffectRow(reduce[0]);
            vertxRowSet.setLastInsertId(reduce[1]);
            return vertxRowSet;
        });
    }

    @Override
    public <R> PreparedQuery<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
        return new SqlResultCollectingPrepareQuery(sql,connection,collector);
    }

    @Override
    public <U> PreparedQuery<RowSet<U>> mapping(Function<Row, U> mapper) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<RowSet<Row>> execute() {
        return new RowSetQuery(sql,connection).execute();
    }

}

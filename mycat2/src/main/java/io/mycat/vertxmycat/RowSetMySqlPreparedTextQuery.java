/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.vertxmycat;

import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.mysqlclient.impl.codec.VertxRowSetImpl;
import io.vertx.sqlclient.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.apply;
import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.toObjects;

public class RowSetMySqlPreparedTextQuery implements AbstractMySqlPreparedQuery<RowSet<Row>> {

    private final String sql;
    private final AbstractMySqlConnection connection;

    public RowSetMySqlPreparedTextQuery(String sql, AbstractMySqlConnection connection) {
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
        return new SqlResultCollectingPrepareTextQuery(sql,connection,collector);
    }

    @Override
    public <U> PreparedQuery<RowSet<U>> mapping(Function<Row, U> mapper) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<RowSet<Row>> execute() {
        return new RowSetQuery(sql,(AbstractMySqlConnectionImpl) connection).execute();
    }

}

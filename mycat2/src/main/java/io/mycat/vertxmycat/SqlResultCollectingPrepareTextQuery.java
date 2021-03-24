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
import io.vertx.sqlclient.*;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.apply;
import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.toObjects;

public class SqlResultCollectingPrepareTextQuery<R> implements AbstractMySqlPreparedQuery<SqlResult<R>> {

    private final String sql;
    private final AbstractMySqlConnection connection;
    private final Collector<Row, Object, R> collector;

    public SqlResultCollectingPrepareTextQuery(String sql,
                                               AbstractMySqlConnection connection,
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

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
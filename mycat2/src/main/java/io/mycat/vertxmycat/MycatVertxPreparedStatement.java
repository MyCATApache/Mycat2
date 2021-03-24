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

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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

public interface AbstractMySqlPreparedQuery<T> extends PreparedQuery<T> {
    @Override
    public default void execute(Tuple tuple, Handler<AsyncResult<T>> handler) {
        Future<T> future = execute();
        if (future!=null){
            future.onComplete(handler);
        }
    }


    @Override
    public default  void executeBatch(List<Tuple> batch, Handler<AsyncResult<T>> handler) {
        Future<T> future = executeBatch(batch);
        if (future!=null){
            future.onComplete(handler);
        }
    }



    @Override
    public default  void execute(Handler<AsyncResult<T>> handler) {
        Future<T>  future = execute();
        if (future!=null){
            future.onComplete(handler);
        }
    }

}

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

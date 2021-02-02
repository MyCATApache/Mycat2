package io.mycat.vertxmycat;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.*;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

public abstract class AbstractMySqlPreparedQuery<T> implements PreparedQuery<T> {
    @Override
    public void execute(Tuple tuple, Handler<AsyncResult<T>> handler) {
        Future<T> future = execute();
        if (future!=null){
            future.onComplete(handler);
        }
    }


    @Override
    public void executeBatch(List<Tuple> batch, Handler<AsyncResult<T>> handler) {
        Future<T> future = executeBatch(batch);
        if (future!=null){
            future.onComplete(handler);
        }
    }



    @Override
    public void execute(Handler<AsyncResult<T>> handler) {
        Future<T>  future = execute();
        if (future!=null){
            future.onComplete(handler);
        }
    }

}

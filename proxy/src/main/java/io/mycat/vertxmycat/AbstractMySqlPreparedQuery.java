package io.mycat.vertxmycat;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.*;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

public abstract class AbstractMySqlPreparedQuery implements PreparedQuery<RowSet<Row>> {
    @Override
    public void execute(Tuple tuple, Handler<AsyncResult<RowSet<Row>>> handler) {
        Future<RowSet<Row>> future = execute();
        if (future!=null){
            future.onComplete(handler);
        }
    }


    @Override
    public void executeBatch(List<Tuple> batch, Handler<AsyncResult<RowSet<Row>>> handler) {
        Future<RowSet<Row>> future = executeBatch(batch);
        if (future!=null){
            future.onComplete(handler);
        }
    }



    @Override
    public void execute(Handler<AsyncResult<RowSet<Row>>> handler) {
        Future<RowSet<Row>>  future = execute();
        if (future!=null){
            future.onComplete(handler);
        }
    }

}

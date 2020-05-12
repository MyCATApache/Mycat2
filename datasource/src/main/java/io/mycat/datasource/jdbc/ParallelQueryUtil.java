package io.mycat.datasource.jdbc;

import io.mycat.ExecutorUtil;
import io.mycat.NameableExecutor;
import io.mycat.RootHelper;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.future.AsyncResult;
import io.mycat.future.Future;
import io.mycat.future.Handler;
import io.mycat.future.Promise;

public class ParallelQueryUtil {
    final static NameableExecutor JDBC_EXECUTOR = ExecutorUtil.create("jdbcExecutor", RootHelper.INSTANCE.getConfigProvider().currentConfig().getServer().getWorker().getMaxThread());

    /**
     * @param connection       必须在主线程获取
     * @param sql
     * @param mycatRowMetaData
     * @return
     */
    public static Future<RowBaseIterator> query(DefaultConnection connection, String sql, MycatRowMetaData mycatRowMetaData) {
        Promise<RowBaseIterator> promise = Promise.promise();
        JDBC_EXECUTOR.submit(() -> {
            try {
                promise.complete(connection.executeQuery(mycatRowMetaData,sql));
            } catch (Throwable e) {
                promise.fail(e);
            }
        });
        return promise.future();
    }

    public static void main(String[] args) {
        Future<RowBaseIterator> query = query(null, null, null);
        Promise<RowBaseIterator> promise = Promise.promise();
        query.setHandler(new Handler<AsyncResult<RowBaseIterator>>() {
            @Override
            public void handle(AsyncResult<RowBaseIterator> event) {
                if (event.succeeded()){
                    System.out.println();
                }else {
                    System.out.println();
                }
            }
        });
    }

}
package io.mycat.connectionschedule;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.sqlclient.SqlConnection;

import java.util.HashMap;
import java.util.Map;

public class SequenceSchedulePolicy implements SchedulePolicy {
    final Map<String, Future<SqlConnection>> futures = new HashMap<>();

    @Override
    public Future<SqlConnection> getConnetion(MycatDataContext dataContext,
                                              int order, int refCount, String targetArg, long deadline,
                                              Future<SqlConnection> recycleConnectionFuture) {
        synchronized (futures) {
            String target = dataContext.resolveDatasourceTargetName(targetArg, true);
            Future<SqlConnection> sqlConnectionFuture = futures.get(target);
            if (sqlConnectionFuture == null) {
                futures.put(target, recycleConnectionFuture);
                XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
                return transactionSession.getConnection(target);
            } else {
                Promise<SqlConnection> promise = Promise.promise();
                futures.put(target, recycleConnectionFuture);
                sqlConnectionFuture.onComplete(promise);
                return promise.future();
            }
        }
    }
}

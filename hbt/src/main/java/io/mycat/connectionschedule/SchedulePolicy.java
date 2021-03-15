package io.mycat.connectionschedule;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.MycatDataContext;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

public interface SchedulePolicy {
        Future<SqlConnection> getConnetion(MycatDataContext context, int order, int refCount, String target, long deadline,
                                           Future<SqlConnection> recycleConnectionFuture);
    }
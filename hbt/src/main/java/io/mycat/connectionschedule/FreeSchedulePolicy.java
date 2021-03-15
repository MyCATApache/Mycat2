package io.mycat.connectionschedule;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.MycatDataContext;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

public class FreeSchedulePolicy implements SchedulePolicy{
    @Override
    public Future<SqlConnection> getConnetion(MycatDataContext dataContext, int order, int refCount, String target, long deadline, Future<SqlConnection> recycleConnectionFuture) {
        return null;
    }
}

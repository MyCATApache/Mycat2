package io.mycat.replica.heart.callback;

import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.client.resultset.QueryResultSetCollector;
import io.mycat.proxy.task.client.resultset.QueryResultSetTask;
import io.mycat.replica.heart.DatasourceStatus;
import io.mycat.replica.heart.HeartBeatAsyncTaskCallBack;
import io.mycat.replica.heart.HeartbeatDetector;

/**
 * @author : zhangwy
 * @date Date : 2019年05月15日 21:34
 */
public class SingleHeartBeatAsyncTaskCallBack extends HeartBeatAsyncTaskCallBack {

    String sql  = "select user()";
    public SingleHeartBeatAsyncTaskCallBack(HeartbeatDetector heartbeatDetector) {
        super(heartbeatDetector);
    }
    @Override
    public void finished(MySQLClientSession mysql, Object sender, boolean success, Object result, Object attr) {
        if( isQuit == false) {
            QueryResultSetTask queryResultSetTask = new QueryResultSetTask();
            QueryResultSetCollector queryResultSetCollector = new QueryResultSetCollector();
            queryResultSetTask
                    .request(mysql, sql,
                            value -> {
                                return value == 0;
                            }, queryResultSetCollector, (session, sender1, success1, result1, errorMessage1) -> {
                            try {
                                if(isQuit == false) {
                                    DatasourceStatus datasourceStatus = new DatasourceStatus();
                                    if (success1) {
                                        queryResultSetCollector.toString();
                                        heartbeatDetector.getHeartbeatManager().setStatus(datasourceStatus, DatasourceStatus.OK_STATUS);
                                    } else {
                                        heartbeatDetector.getHeartbeatManager().setStatus(datasourceStatus, DatasourceStatus.ERROR_STATUS);
                                    }
                                }
                            } finally {
                                session.getSessionManager().addIdleSession(session);
                            }
                });
        }
    }
}

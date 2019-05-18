package io.mycat.replica.heart.callback;

import io.mycat.config.GlobalConfig;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.client.resultset.QueryResultSetCollector;
import io.mycat.proxy.task.client.resultset.QueryResultSetTask;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heart.DatasourceStatus;
import io.mycat.replica.heart.HeartBeatAsyncTaskCallBack;
import io.mycat.replica.heart.HeartbeatDetector;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : zhangwy
 * @date Date : 2019年05月15日 21:34
 */
public class MasterSlaveBeatAsyncTaskCallBack extends HeartBeatAsyncTaskCallBack {
    private static final Logger logger = LoggerFactory.getLogger(MySQLDatasource.class);
    final int slaveThreshold = 1000; //延迟阈值
    public MasterSlaveBeatAsyncTaskCallBack(HeartbeatDetector heartbeatDetector) {
        super(heartbeatDetector);
    }
    @Override
    public void finished(MySQLClientSession mysql, Object sender, boolean success, Object result, Object attr) {
        if( isQuit == false) {
            QueryResultSetTask queryResultSetTask = new QueryResultSetTask();
            QueryResultSetCollector queryResultSetCollector = new QueryResultSetCollector();
            queryResultSetTask
                    .request(mysql, getSql(),
                            value -> {
                                return true;
                            }, queryResultSetCollector, (session, sender1, success1, result1, errorMessage1) -> {
                            try {
                                if(isQuit == false) {
                                    if (success1) {
                                        process(session, sender1, success1, result1, errorMessage1, queryResultSetCollector);
                                    } else {
                                        heartbeatDetector.getHeartbeatManager().setStatus(new DatasourceStatus(), DatasourceStatus.ERROR_STATUS);
                                    }
                                }
                            } finally {
                                session.getSessionManager().addIdleSession(session);
                            }
                });
        }
    }
    public String getSql() {
        return GlobalConfig.MASTER_SLAVE_HEARTBEAT_SQL;
    }
    protected void process(MySQLClientSession session, Object sender1, boolean success1,
        Object result1, Object errorMessage1, QueryResultSetCollector queryResultSetCollector) {
        queryResultSetCollector.toString();
        DatasourceStatus datasourceStatus = new DatasourceStatus();
        List<Map<String, Object>> resultList = queryResultSetCollector.toList();
        if(resultList.size() > 0) {
            Map<String, Object> resultResult = resultList.get(0);
            String Slave_IO_Running  = resultResult != null ? (String)resultResult.get("Slave_IO_Running") : null;
            String Slave_SQL_Running = resultResult != null ? (String)resultResult.get("Slave_SQL_Running") : null;
            if (Slave_IO_Running != null
                    && Slave_IO_Running.equals(Slave_SQL_Running)
                    && Slave_SQL_Running.equals("Yes")) {
                datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_NORMAL);
                Long Behind_Master = (Long)resultResult.get( "Seconds_Behind_Master");
                if ( Behind_Master >  slaveThreshold ) {
                    datasourceStatus.setSlaveBehindMaster(true);
                    System.out.println("found MySQL master/slave Replication delay !!! "+
                            " binlog sync time delay: " + Behind_Master + "s" );
                } else {
                    datasourceStatus.setSlaveBehindMaster(false);
                }
            } else if( heartbeatDetector.getDataSource().isSlave()) {
                String Last_IO_Error = resultResult != null ? (String)resultResult.get("Last_IO_Error") : null;
                System.out.println("found MySQL master/slave Replication err !!! "
                        +   Last_IO_Error);
                datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_ERROR);
            }
        }
        heartbeatDetector.getHeartbeatManager().setStatus(datasourceStatus, DatasourceStatus.OK_STATUS);
    }
}

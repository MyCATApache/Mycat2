package io.mycat.replica.heart.callback;

import io.mycat.collector.CollectorUtil;
import io.mycat.collector.OneResultSetCollector;
import io.mycat.config.GlobalConfig;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heart.DatasourceStatus;
import io.mycat.replica.heart.HeartbeatDetector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : zhangwy
 * @date Date : 2019年05月17日 21:34
 */
public class GaleraHeartBeatAsyncTaskCallBack extends MasterSlaveBeatAsyncTaskCallBack {
    private static final Logger logger = LoggerFactory.getLogger(MySQLDatasource.class);
    public GaleraHeartBeatAsyncTaskCallBack(HeartbeatDetector heartbeatDetector) {
        super(heartbeatDetector);
    }

    public String getSql() {
        return GlobalConfig.GARELA_CLUSTER_HEARTBEAT_SQL;
    }

    protected void process(MySQLClientSession session,
        OneResultSetCollector queryResultSetCollector) {
        queryResultSetCollector.toString();
        DatasourceStatus datasourceStatus = new DatasourceStatus();
        List<Map<String, Object>> resultList = CollectorUtil.toList(queryResultSetCollector);
        Map<String, Object> resultResult = new HashMap<>();
        for(Map<String, Object> map : resultList ) {
            String variableName = (String)map.get("Variable_name");
            String value = (String)map.get("Value");
            resultResult.put(variableName, value);
        }
        if(resultList.size() > 0) {
            String wsrep_cluster_status = resultResult != null ? (String)resultResult.get("wsrep_cluster_status") : null;// Primary
            String wsrep_connected = resultResult != null ? (String)resultResult.get("wsrep_connected") : null;// ON
            String wsrep_ready = resultResult != null ? (String)resultResult.get("wsrep_ready") : null;// ON
            if ("ON".equals(wsrep_connected)
                && "ON".equals(wsrep_ready)
                && "Primary".equals(wsrep_cluster_status)) {
                datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_NORMAL);
                datasourceStatus.setStatus(DatasourceStatus.OK_STATUS);
            } else {
                logger.info("found MySQL  cluster status err !!! "
                    + " wsrep_cluster_status: "+ wsrep_cluster_status
                    + " wsrep_connected: "+ wsrep_connected
                    + " wsrep_ready: "+ wsrep_ready
                );
                datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_ERROR);
                datasourceStatus.setStatus(DatasourceStatus.ERROR_STATUS);
            }
        }
        heartbeatDetector.getHeartbeatManager().setStatus(datasourceStatus, DatasourceStatus.OK_STATUS);
    }
}

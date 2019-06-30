/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.replica.heartbeat.callback;

import io.mycat.collector.CollectorUtil;
import io.mycat.collector.OneResultSetCollector;
import io.mycat.config.GlobalConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartbeatDetector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : zhangwy
 *  date Date : 2019年05月17日 21:34
 */
public class GaleraHeartBeatAsyncTaskCallBack extends MasterSlaveBeatAsyncTaskCallBack {

    private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLDatasource.class);
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
                LOGGER.info("found MySQL  cluster status err !!! "
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

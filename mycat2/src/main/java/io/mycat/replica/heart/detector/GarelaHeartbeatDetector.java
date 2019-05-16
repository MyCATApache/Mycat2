/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.replica.heart.detector;

import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.heart.DatasourceStatus;
import io.mycat.replica.heart.HeartBeatAsyncTaskCallBack;
import io.mycat.replica.heart.HeartbeatDetector;
import io.mycat.replica.heart.HeartbeatManager;
import io.mycat.replica.heart.callback.SingleHeartBeatAsyncTaskCallBack;

import java.util.List;
import java.util.Map;


public class GarelaHeartbeatDetector extends MasterSlaveHeartbeatDetector implements HeartbeatDetector {
    private final int slaveThreshold = 1000;
    public GarelaHeartbeatDetector(ReplicaConfig replicaConfig, MySQLDataSourceEx dataSource, HeartbeatManager heartbeatManager) {
        super(replicaConfig, dataSource , heartbeatManager);

    }

    protected DatasourceStatus processHearbeatResult(List<Map<String, String>> resultList) {
        DatasourceStatus datasourceStatus = new DatasourceStatus();
         Map<String,String>  resultResult= resultList.get(0);
        String wsrep_cluster_status = resultResult != null ? resultResult.get("wsrep_cluster_status") : null;// Primary
        String wsrep_connected = resultResult != null ? resultResult.get("wsrep_connected") : null;// ON
        String wsrep_ready = resultResult != null ? resultResult.get("wsrep_ready") : null;// ON
        if ("ON".equals(wsrep_connected)
                && "ON".equals(wsrep_ready)
                && "Primary".equals(wsrep_cluster_status)) {

            datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_NORMAL);
            datasourceStatus.setStatus(DatasourceStatus.OK_STATUS);

        } else {
            System.out.println("found MySQL  cluster status err !!! "
                    + " wsrep_cluster_status: "+ wsrep_cluster_status
                    + " wsrep_connected: "+ wsrep_connected
                    + " wsrep_ready: "+ wsrep_ready
            );
            datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_ERROR);
            datasourceStatus.setStatus(DatasourceStatus.ERROR_STATUS);

        }
        return datasourceStatus;
    }
    @Override
    public HeartBeatAsyncTaskCallBack getAsyncTaskCallback() {
        return new SingleHeartBeatAsyncTaskCallBack(this);
    }
//
//    public HeartbeatInfReceiver<DatasourceStatus> getHeartbeatInfReceiver() {
//        return datasourceStatus -> {
//            return DatasourceStatus.OK_STATUS == datasourceStatus.getStatus()
//                    && datasourceStatus.getDbSynStatus() == DatasourceStatus.OK_STATUS;
//        };
//    }

}

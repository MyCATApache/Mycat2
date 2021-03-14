/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.replica.heartbeat.strategy;

import io.mycat.GlobalConst;
import io.mycat.api.collector.CommonSQLCallback;
import io.mycat.replica.heartbeat.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author : zhangwy
 * @author : chenujunwen date Date : 2019年05月17日 21:34
 */
public class MySQLGaleraHeartBeatStrategy extends HeartBeatStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLGaleraHeartBeatStrategy.class);

    public List<String> getSqls() {
        return Collections.singletonList(
                GlobalConst.GARELA_CLUSTER_HEARTBEAT_SQL);
    }

    @Override
    public void process(List<List<Map<String, Object>>> resultList) {
        if (!resultList.isEmpty()){
            List<Map<String, Object>> result = resultList.get(0);

            DatasourceStatus datasourceStatus = new DatasourceStatus();

            datasourceStatus.setMaster(heartbeatFlow.getInstance().isMaster());

            Map<String, Object> resultResult = new HashMap<>();
            for (Map<String, Object> map : result) {
                String variableName = (String) map.get("Variable_name");
                String value = (String) map.get("Value");
                resultResult.put(variableName, value);
            }
            if (resultList.size() > 0) {
                String wsrep_cluster_status = (String) resultResult.get("wsrep_cluster_status");// Primary
                String wsrep_connected = (String) resultResult.get("wsrep_connected");// ON
                String wsrep_ready = (String) resultResult.get("wsrep_ready");// ON

                if ("ON".equals(wsrep_connected)
                        && "ON".equals(wsrep_ready)
                        && "Primary".equals(wsrep_cluster_status)) {
                    datasourceStatus.setDbSynStatus(DbSynEnum.DB_SYN_NORMAL);
                } else {
                    LOGGER.info("found MySQL  cluster status err !!! "
                            + " wsrep_cluster_status: " + wsrep_cluster_status
                            + " wsrep_connected: " + wsrep_connected
                            + " wsrep_ready: " + wsrep_ready
                    );
                    datasourceStatus.setDbSynStatus(DbSynEnum.DB_SYN_ERROR);
                }
                try {
                    BigDecimal wsrep_flow_control_paused = new BigDecimal(
                            Objects.toString(resultResult.get("wsrep_flow_control_paused")));// delay
                    if (wsrep_flow_control_paused.compareTo(BigDecimal.valueOf(heartbeatFlow.getSlaveThreshold())) > 0) {
                        datasourceStatus.setSlaveBehindMaster(true);
                        LOGGER.warn("found MySQL Galera master/slave Replication delay !!! " +
                                " delay: " + wsrep_flow_control_paused + "s");
                    } else {
                        datasourceStatus.setSlaveBehindMaster(false);
                    }
                } catch (Throwable throwable) {
                    LOGGER.error("", throwable);
                }
            }
            heartbeatFlow.setStatus(datasourceStatus, DatasourceEnum.OK_STATUS);
        }

    }

    @Override
    public void onException(Exception e) {
        heartbeatFlow.setStatus(new DatasourceStatus(),DatasourceEnum.ERROR_STATUS);
    }

    public MySQLGaleraHeartBeatStrategy() {
    }

    public MySQLGaleraHeartBeatStrategy(HeartbeatFlow heartbeatFlow) {
        super(heartbeatFlow);
    }
}

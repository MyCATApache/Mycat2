/**
 * Copyright (C) <2021>  <chen junwen>
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

import io.mycat.replica.heartbeat.DatasourceEnum;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.replica.heartbeat.HeartbeatFlow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author : chenujunwen
 */
public class MGRHeartBeatStrategy extends HeartBeatStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(MGRHeartBeatStrategy.class);

    public static String CHECK_SQL = "SELECT (SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE member_id=@@server_uuid) AS `MEMBER_STATE`,\n" +
            "(SELECT TRUE FROM performance_schema.global_variables WHERE variable_name IN ('read_only', 'super_read_only') AND VARIABLE_VALUE = 'ON' LIMIT 1) AS `READ_ONLY`,\n" +
            "(SELECT (Received_transaction_set-@@gtid_executed) FROM performance_schema.replication_connection_status WHERE Channel_name = 'group_replication_applier') AS `BEHIND`";

    public List<String> getSqls() {
        return Collections.singletonList(
                CHECK_SQL
        );
    }

    @Override
    public void process(List<List<Map<String, Object>>> resultList) {
        DatasourceStatus datasourceStatus = new DatasourceStatus();
        Map<String, Object> result = resultList.get(0).get(0);
        boolean master = !("1".equalsIgnoreCase(Objects.toString( result.getOrDefault("READ_ONLY", null))));
        double behind;
        if (!"ONLINE".equalsIgnoreCase(Objects.toString(result.getOrDefault("MEMBER_STATE","OFFLINE")))) {
            heartbeatFlow.setStatus(datasourceStatus, DatasourceEnum.ERROR_STATUS);
            return;
        }
        {
            behind = ((Number) (result.get("BEHIND"))).doubleValue();
            double delay;
            if ((delay = (behind - heartbeatFlow.getSlaveThreshold())) > 0) {
                datasourceStatus.setSlaveBehindMaster(true);
                LOGGER.warn("found MySQL MGR GTID delay !!! " +
                        " delay: " + delay);
            } else {
                datasourceStatus.setSlaveBehindMaster(false);
            }
        }
        datasourceStatus.setMaster(master);
        heartbeatFlow.setStatus(datasourceStatus, DatasourceEnum.OK_STATUS);
    }

    private boolean isOnline(List<Map<String, Object>> group_replication_primary_member_resultset) {
        boolean online;
        if (group_replication_primary_member_resultset.isEmpty()) {
            LOGGER.error("found MGR  cluster status err !!! ");
            online = false;
        } else {
            Map<String, String> info = (Map) group_replication_primary_member_resultset.get(0);
            String member_state = (String) info.get("MEMBER_STATE");
            online = ("ONLINE".equalsIgnoreCase(member_state));
        }
        return online;
    }

    @Override
    public void onException(Exception e) {
        heartbeatFlow.setStatus(new DatasourceStatus(),DatasourceEnum.ERROR_STATUS);
    }

    public MGRHeartBeatStrategy() {
    }

    public MGRHeartBeatStrategy(HeartbeatFlow heartbeatFlow) {
        super(heartbeatFlow);
    }
}

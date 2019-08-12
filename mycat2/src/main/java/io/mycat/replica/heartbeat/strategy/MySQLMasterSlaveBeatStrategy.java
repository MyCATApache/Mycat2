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
package io.mycat.replica.heartbeat.strategy;

import io.mycat.api.collector.CommonSQLCallback;
import io.mycat.config.GlobalConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartbeatDetector;
import java.util.List;
import java.util.Map;

/**
 * @author : zhangwy date Date : 2019年05月15日 21:34
 */
public class MySQLMasterSlaveBeatStrategy implements CommonSQLCallback {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(
      MySQLMasterSlaveBeatStrategy.class);
  final long slaveThreshold; //延迟阈值
  protected final HeartbeatDetector heartbeatDetector;

  public MySQLMasterSlaveBeatStrategy(HeartbeatDetector heartbeatDetector) {
    this.heartbeatDetector = heartbeatDetector;
    this.slaveThreshold = heartbeatDetector.getReplicaConfig().getSlaveThreshold();
  }

  public String getSql() {
    return GlobalConfig.MASTER_SLAVE_HEARTBEAT_SQL;
  }

  public void process(List<Map<String, Object>> resultList) {
    DatasourceStatus datasourceStatus = new DatasourceStatus();
    if (resultList.size() > 0) {
      Map<String, Object> resultResult = resultList.get(0);
      String Slave_IO_Running =
          resultResult != null ? (String) resultResult.get("Slave_IO_Running") : null;
      String Slave_SQL_Running =
          resultResult != null ? (String) resultResult.get("Slave_SQL_Running") : null;
      if (Slave_IO_Running != null
          && Slave_IO_Running.equals(Slave_SQL_Running)
          && Slave_SQL_Running.equals("Yes")) {
        datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_NORMAL);
        Long Behind_Master = (Long) resultResult.get("Seconds_Behind_Master");
        if (Behind_Master > slaveThreshold) {
          datasourceStatus.setSlaveBehindMaster(true);
          LOGGER.info("found MySQL master/slave Replication delay !!! " +
              " binlog sync time delay: " + Behind_Master + "s");
        } else {
          datasourceStatus.setSlaveBehindMaster(false);
        }
      } else if (heartbeatDetector.getDataSource().instance().asSelectRead()) {
        String Last_IO_Error =
            resultResult != null ? (String) resultResult.get("Last_IO_Error") : null;
        System.out.println("found MySQL master/slave Replication err !!! "
            + Last_IO_Error);
        datasourceStatus.setDbSynStatus(DatasourceStatus.DB_SYN_ERROR);
      }
    }
    heartbeatDetector.getHeartbeatManager().setStatus(datasourceStatus, DatasourceStatus.OK_STATUS);
  }

  @Override
  public void onError(String errorMessage) {
    heartbeatDetector.getHeartbeatManager().setStatus(DatasourceStatus.ERROR_STATUS);
  }

  @Override
  public void onException(Exception e) {
    heartbeatDetector.getHeartbeatManager().setStatus(DatasourceStatus.ERROR_STATUS);
  }

}

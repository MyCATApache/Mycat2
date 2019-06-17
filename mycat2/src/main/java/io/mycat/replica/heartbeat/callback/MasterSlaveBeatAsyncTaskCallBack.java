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

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.collector.CollectorUtil;
import io.mycat.collector.OneResultSetCollector;
import io.mycat.collector.TextResultSetTransforCollector;
import io.mycat.config.GlobalConfig;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.backend.TextResultSetHandler;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatAsyncTaskCallBack;
import io.mycat.replica.heartbeat.HeartbeatDetector;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : zhangwy
 *  date Date : 2019年05月15日 21:34
 */
public class MasterSlaveBeatAsyncTaskCallBack extends HeartBeatAsyncTaskCallBack {

  private static final Logger logger = LoggerFactory.getLogger(MySQLDatasource.class);
  final int slaveThreshold = 1000; //延迟阈值

  public MasterSlaveBeatAsyncTaskCallBack(HeartbeatDetector heartbeatDetector) {
    super(heartbeatDetector);
  }

  public String getSql() {
    return GlobalConfig.MASTER_SLAVE_HEARTBEAT_SQL;
  }

  protected void process(OneResultSetCollector queryResultSetCollector) {
    DatasourceStatus datasourceStatus = new DatasourceStatus();
    List<Map<String, Object>> resultList = CollectorUtil.toList(queryResultSetCollector);
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
          System.out.println("found MySQL master/slave Replication delay !!! " +
                                 " binlog sync time delay: " + Behind_Master + "s");
        } else {
          datasourceStatus.setSlaveBehindMaster(false);
        }
      } else if (heartbeatDetector.getDataSource().isSlave()) {
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
  public void onSession(MySQLClientSession session, Object sender, Object attr) {
    if (isQuit == false) {
      OneResultSetCollector collector = new OneResultSetCollector();
      TextResultSetTransforCollector transfor = new TextResultSetTransforCollector(
          collector);
      TextResultSetHandler queryResultSetTask = new TextResultSetHandler(transfor, (i) -> true);
      queryResultSetTask.request(session, MySQLCommandType.COM_QUERY, getSql(),
          new ResultSetCallBack<MySQLClientSession>() {
            @Override
            public void onFinishedSendException(Exception exception, Object sender,
                Object attr) {
              if (isQuit == false) {
                onStatus(DatasourceStatus.ERROR_STATUS);
              }
            }

            @Override
            public void onFinishedException(Exception exception, Object sender, Object attr) {
              if (isQuit == false) {
                onStatus(DatasourceStatus.ERROR_STATUS);
              }

            }

            @Override
            public void onFinished(boolean monopolize, MySQLClientSession mysql,
                Object sender, Object attr) {
              try {
                if (isQuit == false) {
                  process(collector);
                }
              } finally {
                mysql.getSessionManager().addIdleSession(mysql);
              }
            }

            @Override
            public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                MySQLClientSession mysql, Object sender, Object attr) {
              if (isQuit == false) {
                onStatus(DatasourceStatus.ERROR_STATUS);
              }
            }
          })
      ;
    }
  }

  @Override
  public void onException(Exception e, Object sender, Object attr) {
    if (isQuit == false) {
      onStatus(DatasourceStatus.ERROR_STATUS);
    }
  }
}

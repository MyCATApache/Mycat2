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
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartbeatDetector;
import java.util.List;
import java.util.Map;

/**
 * @author : zhangwy date Date : 2019年05月15日 21:34
 */
public class MySQLSingleHeartBeatStrategy implements CommonSQLCallback {

  final static String sql = "select user()";
  final private HeartbeatDetector heartbeatDetector;

  public MySQLSingleHeartBeatStrategy(HeartbeatDetector heartbeatDetector) {
    this.heartbeatDetector = heartbeatDetector;
  }


  @Override
  public String getSql() {
    return sql;
  }

  @Override
  public void process(List<Map<String, Object>> resultSetList) {

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

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

import io.mycat.replica.heartbeat.DatasourceEnum;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.replica.heartbeat.HeartbeatFlow;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author : zhangwy date Date : 2019年05月15日 21:34
 */
public class MySQLSingleHeartBeatStrategy extends HeartBeatStrategy {

  final static String sql = "select 1";

  @Override
  public List<String> getSqls() {
    return Collections.singletonList(sql);
  }

  @Override
  public void process(List<List<Map<String, Object>>> resultSetList) {
    this.heartbeatFlow.setStatus(new DatasourceStatus(),DatasourceEnum.OK_STATUS);
  }

  @Override
  public void onException(Exception e) {
    this.heartbeatFlow.setStatus(new DatasourceStatus(),DatasourceEnum.ERROR_STATUS);
  }

  public MySQLSingleHeartBeatStrategy() {
  }

  public MySQLSingleHeartBeatStrategy(HeartbeatFlow heartbeatFlow) {
    super(heartbeatFlow);
  }
}

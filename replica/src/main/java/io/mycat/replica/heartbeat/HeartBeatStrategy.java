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
package io.mycat.replica.heartbeat;

import io.mycat.api.collector.CommonSQLCallback;

/**
 * @author : zhangwy date Date : 2019年05月15日 21:34
 */
public abstract class HeartBeatStrategy implements CommonSQLCallback {

  protected HeartbeatFlow heartbeatFlow;
  protected volatile boolean quit = false;

  public HeartBeatStrategy() {
  }

  public HeartBeatStrategy(HeartbeatFlow heartbeatFlow) {
    this.heartbeatFlow = heartbeatFlow;
  }

  public void setHeartbeatFlow(HeartbeatFlow heartbeatFlow) {
    this.heartbeatFlow = heartbeatFlow;
  }

  public boolean isQuit() {
    return quit;
  }

  public void setQuit(boolean quit) {
    this.quit = quit;
  }

  public void onStatus(DatasourceState status) {
    if (heartbeatFlow != null && !quit) {
      heartbeatFlow.setStatus(status);
    }
  }
}
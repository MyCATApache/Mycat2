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
package io.mycat.thread;

import io.mycat.bindThread.BindThread;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.bindThread.BindThreadPool;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author Junwen Chen
 **/
public class GThreadPool<KEY extends BindThreadKey> extends BindThreadPool<KEY, BindThread> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GThreadPool.class);
  public GThreadPool(JdbcRuntime runtime) {
    super(runtime.getMaxPengdingLimit(), runtime.getWaitTaskTimeout(),
        TimeUnit.valueOf(runtime.getTimeUnit()), runtime.getMaxThread(), runtime.getMaxThread(),
        bindThreadPool -> new GThread( bindThreadPool), (e) -> LOGGER.error("", e));
  }

  public GThreadPool(int maxPengdingLimit, long waitTaskTimeout, TimeUnit timeoutUnit, int minThread, int maxThread) {
    super(maxPengdingLimit, waitTaskTimeout, timeoutUnit, minThread, maxThread, bindThreadPool -> new GThread( bindThreadPool), (e) -> LOGGER.error("", e));
  }

  public GThreadPool(ServerConfig.Worker worker) {
    super(worker.getMaxPengdingLimit(), worker.getWaitTaskTimeout(), TimeUnit.valueOf(worker.getTimeUnit()), worker.getMinThread(), worker.getMaxThread(),  bindThreadPool -> new GThread( bindThreadPool), (e) -> LOGGER.error("", e));
  }
}
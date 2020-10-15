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

import io.mycat.MycatDataContext;
import io.mycat.bindthread.BindThread;
import io.mycat.bindthread.BindThreadCallback;
import io.mycat.bindthread.BindThreadPool;
import io.mycat.config.ThreadPoolExecutorConfig;
import io.mycat.proxy.session.MycatContextThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Junwen Chen
 **/
public class SimpleMycatContextBindingThreadPool extends BindThreadPool<MycatDataContext, BindThread> implements MycatContextThreadPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMycatContextBindingThreadPool.class);


  public SimpleMycatContextBindingThreadPool(int maxPengdingLimit,
                                             long waitTaskTimeout,
                                             TimeUnit timeoutUnit,
                                             int minThread,
                                             int maxThread,
                                             long keeplive,
                                             ExecutorService executorService) {
    super(maxPengdingLimit, waitTaskTimeout, timeoutUnit, minThread, maxThread,keeplive, bindThreadPool -> new GThread( bindThreadPool),executorService, (e) -> LOGGER.error("", e));
  }

  public SimpleMycatContextBindingThreadPool(ThreadPoolExecutorConfig worker, ExecutorService executorService) {
    super(worker.getMaxPendingLimit(), worker.getTaskTimeout(), TimeUnit.valueOf(worker.getTimeUnit()), worker.getCorePoolSize(), worker.getMaxPoolSize(),worker.getKeepAliveTime(), bindThreadPool -> new GThread( bindThreadPool), executorService,(e) -> LOGGER.error("", e));
  }


  @Override
  public void run(MycatDataContext container, BindThreadCallback bindThreadCallback) {
    super.run(container,bindThreadCallback);
  }

  @Override
  public void runOnBinding(MycatDataContext container, BindThreadCallback bindThreadCallback) {
    super.runOnBinding(container, bindThreadCallback);
  }
}
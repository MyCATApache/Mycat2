/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.proxy.reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class ReactorEnvThread extends Thread implements SessionThread {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReactorEnvThread.class);
  protected final ConcurrentLinkedQueue<NIOJob> pendingJobs = new ConcurrentLinkedQueue<>();


  public ReactorEnvThread() {
  }

  public ReactorEnvThread(Runnable target) {
    super(target);
  }

  public ReactorEnvThread(Runnable target, String name) {
    super(target, name);
  }


  /**
   * 向pending队列添加任务
   */
  public void addNIOJob(NIOJob job) {
    pendingJobs.offer(job);
    wakeup();
  }
 public abstract void wakeup();

  protected void processNIOJob() {
    NIOJob nioJob = null;
    ReactorEnvThread reactor = this;
    while ((nioJob = pendingJobs.poll()) != null) {
      try {
        nioJob.run(reactor);
      } catch (Exception e) {
        LOGGER.error("Run nio job err:{}", e);
        nioJob.stop(reactor, e);
      }
    }
  }



  public void close(Exception throwable) {
    Objects.requireNonNull(throwable);
    for (NIOJob pendingJob : pendingJobs) {
      try {
        pendingJob.stop(this, throwable);
      } catch (Exception e) {
        LOGGER.error("when close {} but occur exception", this);
      }
    }
  }
}
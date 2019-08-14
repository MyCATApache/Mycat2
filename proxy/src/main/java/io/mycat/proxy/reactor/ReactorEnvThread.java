package io.mycat.proxy.reactor;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class ReactorEnvThread extends Thread implements SessionThread {

  protected final static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(ReactorEnvThread.class);
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
  }


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
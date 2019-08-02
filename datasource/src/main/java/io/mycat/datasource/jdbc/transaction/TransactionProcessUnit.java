package io.mycat.datasource.jdbc.transaction;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.ReactorEnvThread;
import java.util.concurrent.LinkedTransferQueue;

public final class TransactionProcessUnit extends ReactorEnvThread {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(TransactionProcessUnit.class);
  private volatile long startTime;
  private final LinkedTransferQueue<Runnable> blockingDeque = new LinkedTransferQueue<>();


  public void run(Runnable runnale) {
    blockingDeque.offer(runnale);
  }

  @Override
  public void run() {

    while (true) {
      Runnable poll = null;
      try {
        poll = blockingDeque.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (poll != null) {
        this.startTime = System.currentTimeMillis();
        try {
          poll.run();
        } catch (Exception e) {
          LOGGER.error("",e);
        }
      }
    }
  }

  public long getStartTime() {
    return startTime;
  }
}
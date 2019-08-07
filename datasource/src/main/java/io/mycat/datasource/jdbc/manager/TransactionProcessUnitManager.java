package io.mycat.datasource.jdbc.manager;

import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionProcessUnitManager {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(TransactionProcessUnitManager.class);
  final ConcurrentHashMap<TransactionProcessKey, TransactionProcessUnit> map = new ConcurrentHashMap<>();
  final ConcurrentLinkedQueue<TransactionProcessUnit> idleList = new ConcurrentLinkedQueue<>();
  final ConcurrentLinkedQueue<TransactionProcessUnit> allSession = new ConcurrentLinkedQueue<>();
  final LinkedTransferQueue<Runnable> pending = new LinkedTransferQueue<>();
  final AtomicInteger threadCounter = new AtomicInteger();
  final int maxThread = 50;
  final GRuntime runtime;
  private final ScheduledExecutorService service;

  public TransactionProcessUnitManager(GRuntime runtime) {
    this.runtime = runtime;
    this.service = Executors.newScheduledThreadPool(1);
    this.service.scheduleAtFixedRate(() -> {
      try {
        pollTask();
      } catch (Exception e) {
        LOGGER.error("", e);
      }
    }, 1, 5, TimeUnit.SECONDS);
  }

  public void run(TransactionProcessKey key, TransactionProcessTask task) {
    TransactionProcessUnit transactionThread = map.get(key);
    if (transactionThread == null) {
      transactionThread = idleList.poll();
      if (transactionThread == null) {
        if (tryIncThreadCount()) {
          transactionThread = new TransactionProcessUnit(runtime, this);
          transactionThread.start();
          map.put(key, transactionThread);
        } else {
          pending.offer(() -> {
            run(key, task);
          });
          return;
        }
      }
    }
    transactionThread.run(key, task);
  }

  private void pollTask() {
    try {
      Runnable poll;
      while ((poll = pending.poll()) != null) {
        poll.run();
      }
    } catch (Exception e) {
      LOGGER.error("", e);
    }
  }


  private boolean tryIncThreadCount() {
    return threadCounter.get() < threadCounter.updateAndGet(operand -> {
      if (maxThread <= operand) {
        return maxThread;
      } else {
        return ++operand;
      }
    });
  }
}
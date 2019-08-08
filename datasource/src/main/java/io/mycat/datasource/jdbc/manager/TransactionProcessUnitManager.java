package io.mycat.datasource.jdbc.manager;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionProcessUnitManager {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(TransactionProcessUnitManager.class);
  final ConcurrentHashMap<TransactionProcessKey, TransactionProcessUnit> map = new ConcurrentHashMap<>();
  final ConcurrentLinkedQueue<TransactionProcessUnit> idleList = new ConcurrentLinkedQueue<>();
  final ConcurrentLinkedQueue<TransactionProcessUnit> allSession = new ConcurrentLinkedQueue<>();
  final LinkedBlockingQueue<PengdingJob> pending;
  final AtomicInteger threadCounter = new AtomicInteger();
  final int minThread = 2;
  final int maxThread = 50;
  final long timeout;
  final TimeUnit timeoutUnit;
  volatile boolean needDecThreadCount;
  final GRuntime runtime;
//  private final ScheduledExecutorService service;

  public TransactionProcessUnitManager(GRuntime runtime) {
    this.runtime = runtime;
    final int maxPengdingLimit = -1;
    this.timeout = 5;
    this.timeoutUnit = TimeUnit.SECONDS;
    this.pending = new LinkedBlockingQueue<>(
        maxPengdingLimit < 0 ? Integer.MAX_VALUE : maxPengdingLimit);
//    this.service = Executors.newScheduledThreadPool(1);
//    this.service.scheduleAtFixedRate(() -> {
//      try {
//        pollTask();
//      } catch (Exception e) {
//        LOGGER.error("", e);
//      }
//    }, 1, 5, TimeUnit.SECONDS);
  }

  void pollTask() {
    PengdingJob poll = null;
    try {
      while ((poll = pending.poll()) != null) {
        if (!poll.run()) {
          break;
        }
      }
      if (needDecThreadCount && map.isEmpty()) {
        idleList.size();
      }
    } catch (Exception e) {
      LOGGER.error("", e);
      if (poll != null) {
        poll.getTask().onException(poll.getKey(), e);
      }
    }
  }


  boolean tryIncThreadCount() {
    return threadCounter.updateAndGet(operand -> {
      if (maxThread < operand) {
        return maxThread;
      } else {
        return ++operand;
      }
    }) < maxThread;
  }

  void decThreadCount() {
    AtomicInteger threadCounter = this.threadCounter;
    threadCounter.updateAndGet(operand -> {
      if (operand > 0) {
        return --operand;
      } else {
        return 0;
      }
    });
  }


  public boolean run(TransactionProcessKey key, TransactionProcessJob task) {
    TransactionProcessUnit transactionThread = map.get(key);
    if (transactionThread == null) {
      transactionThread = idleList.poll();
      if (transactionThread == null) {
        if (tryIncThreadCount()) {
          transactionThread = new TransactionProcessUnit(runtime, this);
          transactionThread.start();
          map.put(key, transactionThread);
        } else {
          if (!pending.offer(createPengdingTask(key, task))) {
            task.onException(key, new MycatException("max pending job limit"));
          }
          return false;
        }
      }
    }
    transactionThread.run(key, task);
    return true;
  }

  private PengdingJob createPengdingTask(TransactionProcessKey key, TransactionProcessJob task) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(task);
    return new PengdingJob() {
      @Override
      public boolean run() {
        return TransactionProcessUnitManager.this.run(key, task);
      }

      @Override
      public TransactionProcessKey getKey() {
        return key;
      }

      @Override
      public TransactionProcessJob getTask() {
        return task;
      }
    };
  }

  public void tryDecThread() {
    while (idleList.size() > minThread) {
      TransactionProcessUnit poll = idleList.poll();
      if (poll != null) {
        poll.close();
        LOGGER.debug("close idle unit");
        allSession.remove(poll);
        decThreadCount();
      } else {
        break;
      }
      }
  }

  private interface PengdingJob {

    boolean run();

    TransactionProcessKey getKey();

    TransactionProcessJob getTask();
  }
}
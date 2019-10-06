package io.mycat.bindThread;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;

public class BindThreadPool<KEY extends BindThreadKey, PROCESS extends BindThread> {

  final ConcurrentHashMap<KEY, PROCESS> map = new ConcurrentHashMap<>();
  final ArrayBlockingQueue<PROCESS> idleList;
  final ArrayBlockingQueue<PengdingJob> pending;
  final Function<BindThreadPool, PROCESS> processFactory;
  final Consumer<Exception> exceptionHandler;
  final AtomicInteger threadCounter = new AtomicInteger(0);
  final int minThread;
  final int maxThread;
  final long waitTaskTimeout;
  final TimeUnit timeoutUnit;
  private final ScheduledExecutorService check;

  public BindThreadPool(int maxPengdingLimit, long waitTaskTimeout,
      TimeUnit timeoutUnit, int minThread, int maxThread,
      Function<BindThreadPool, PROCESS> processFactory,
      Consumer<Exception> exceptionHandler) {
    this.waitTaskTimeout = waitTaskTimeout;
    this.timeoutUnit = timeoutUnit;
    this.minThread = minThread;
    this.maxThread = maxThread+1;
    this.idleList = new ArrayBlockingQueue<>(maxThread);
    this.pending = new ArrayBlockingQueue<>(
        maxPengdingLimit < 0 ? 65535 : maxPengdingLimit);
    this.processFactory = processFactory;
    this.exceptionHandler = exceptionHandler;
    this.check = Executors.newScheduledThreadPool(1);
    this.check.submit(new Runnable() {
                        @Override
                        public void run() {
                          while (true) {
                            try {
                              pollTask();
                            } catch (Exception e) {
                              e.printStackTrace();
                            }
                          }
                        }
                      }
        //   , 1, 1, TimeUnit.MILLISECONDS
    );
  }

  void pollTask() {
    try {
      PROCESS process = idleList.poll(waitTaskTimeout, timeoutUnit);
      if (process != null) {
        idleList.add(process);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    PengdingJob poll = null;
    try {
      while ((poll = pending.poll(waitTaskTimeout, timeoutUnit)) != null) {
        if (!poll.run()) {
          break;
        }
      }
    } catch (Exception e) {
      exceptionHandler.accept(e);
      if (poll != null) {
        poll.getTask().onException(poll.getKey(), e);
      }
    }
    if (poll == null) {
      tryDecThread();
      threadCounter.updateAndGet(new IntUnaryOperator() {
          @Override
          public int applyAsInt(int operand) {
              return map.size()+       idleList .size();
          }
      });
    }
  }


  boolean tryIncThreadCount() {
    return threadCounter.updateAndGet(operand -> {
      if (maxThread <= operand) {
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

  public boolean run(KEY key, BindThreadCallback<KEY, PROCESS> task) {
    PROCESS transactionThread = map.get(key);
    if (transactionThread == null) {
      transactionThread = idleList.poll();
      if (transactionThread == null) {
        if (tryIncThreadCount()) {
          transactionThread = processFactory.apply(this);
          transactionThread.start();
        } else {
          if (!pending.offer(createPengdingTask(key, task))) {
            task.onException(key, new Exception("max pending job limit"));
          }
          return false;
        }
      }
    }
    map.put(key, transactionThread);
    transactionThread.run(key, task);
    return true;
  }

  private PengdingJob createPengdingTask(KEY key, BindThreadCallback task) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(task);
    return new PengdingJob() {
      @Override
      public boolean run() {
        return BindThreadPool.this.run(key, task);
      }

      @Override
      public BindThreadKey getKey() {
        return key;
      }

      @Override
      public BindThreadCallback getTask() {
        return task;
      }
    };
  }

  public void tryDecThread() {
    if (threadCounter.get() - map.size() > minThread) {
      PROCESS poll = idleList.poll();
      if (poll != null) {
        decThreadCount();
        poll.close();
      }
    }
  }

  interface PengdingJob {

    boolean run();

    BindThreadKey getKey();

    BindThreadCallback getTask();
  }
}
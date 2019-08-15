package io.mycat.bindThread;

import java.util.Objects;
import java.util.concurrent.LinkedTransferQueue;

public abstract class BindThread<KEY extends BindThreadKey, PROCESS extends BindThreadCallback> extends
    Thread {

  final LinkedTransferQueue<PROCESS> blockingDeque = new LinkedTransferQueue<>();//todo optimization
  final BindThreadPool manager;
  long startTime;
  volatile KEY key;

  public BindThread(BindThreadPool manager) {
    this.manager = manager;
  }

  protected abstract boolean continueBind();

  void run(KEY key, PROCESS processTask) {
    Objects.requireNonNull(key);
    if (!blockingDeque.isEmpty() && this.key != key) {
      throw new RuntimeException("unknown state");
    } else if (this.key == null) {
      this.key = key;
    } else if (this.key != null && this.key == key) {

    } else {
      throw new RuntimeException("unknown state");
    }
    if (Thread.currentThread() == this) {
      processJob(null, processTask);
    } else {
      blockingDeque.offer(processTask);
    }
  }

  @Override
  public void run() {
    try {
      Exception exception = null;
      BindThreadCallback callback = null;
      while (!isInterrupted()) {
        exception = null;
        callback = null;

        try {
          callback = blockingDeque.poll(manager.waitTaskTimeout, manager.timeoutUnit);
        } catch (InterruptedException ignored) {
        }
        if (callback != null) {
          processJob(exception, callback);

        }

        {
          boolean bind = false;
          if (this.key != null && !(bind = continueBind())) {
            recycleTransactionThread(callback);
          } else if (this.key == null && bind) {
            throw new RuntimeException("unknown state");
          }
        }
      }
    } catch (Exception e) {
      manager.exceptionHandler.accept(e);
    }
  }

  private void processJob(Exception exception, BindThreadCallback poll) {
    this.startTime = System.currentTimeMillis();
    try {
      poll.accept(this.key, this);
    } catch (Exception e) {
      manager.exceptionHandler.accept(e);
      exception = e;
    }
    if (exception != null) {
      poll.onException(key, exception);
    }
  }

  public void recycleTransactionThread(BindThreadCallback callback) {
    if (!continueBind() && callback == null) {
      manager.map.remove(this.key);
      this.key = null;
      if (!manager.idleList.offer(this)) {
        close();
        manager.decThreadCount();
        manager.allSession.remove(this);
      }
    }
  }


  public long getStartTime() {
    return startTime;
  }

  public void close() {
//    super.close();
    this.interrupt();
  }

//  public AutocommitConnection getAutocommitConnection(JdbcDataSource dataSource) {
//    return dataSource.getReplica().getAutocomitConnection(dataSource);
//  }
//
//  public LocalTransactionConnection getLocalTransactionConnection(JdbcDataSource dataSource,
//      int transactionIsolation) {
//    return dataSource.getReplica().getLocalTransactionConnection(dataSource, transactionIsolation);
//  }
//
//  public XATransactionConnection getXATransactionConnection(JdbcDataSource dataSource,
//      int transactionIsolation) {
//    return dataSource.getReplica().getXATransactionConnection(dataSource, transactionIsolation);
//  }
//
//  public TransactionSession getTransactionSession() {
//    return transactionSession;
//  }
//
//
//  public GRuntime getRuntime() {
//    return manager.runtime;
//  }
}
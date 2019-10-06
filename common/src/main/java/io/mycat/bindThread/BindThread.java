package io.mycat.bindThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

public abstract class BindThread<KEY extends BindThreadKey, PROCESS extends BindThreadCallback> extends
    Thread {
  final Logger LOGGER = LoggerFactory.getLogger(BindThread.class);
  final BlockingQueue<PROCESS> blockingDeque = new LinkedTransferQueue<>();//todo optimization
  final BindThreadPool manager;
  long startTime;
  volatile KEY key;
  private long endTime;

  public BindThread(BindThreadPool manager) {
    this.manager = manager;
  }

  protected abstract boolean continueBind();

  void run(KEY key, PROCESS processTask) {
    Objects.requireNonNull(key);
    if (!blockingDeque.isEmpty() &&this.key!=null) {
      throw new RuntimeException("unknown state");
    } else if (this.key == null) {
      this.key = key;
    } else if (this.key != null && this.key == key) {

    } else {
      throw new RuntimeException("unknown state");
    }
    this.key = key;
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

        callback = blockingDeque.poll(this.manager.waitTaskTimeout,this.manager.timeoutUnit);
        if (callback != null) {
          processJob(exception, callback);
        }

        {
          boolean bind = false;
          if (this.key != null && !(bind = continueBind())) {
            recycleTransactionThread();
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
    this.endTime = System.currentTimeMillis();
    LOGGER.debug("thread execute time:{} {} ", this.endTime-this.startTime,"Millis");
    if (exception != null) {
      poll.onException(key, exception);
    }
  }

  public void recycleTransactionThread() {
    if (!continueBind()) {
      manager.map.remove(this.key);
      this.key = null;
      if (!manager.idleList.offer(this)) {
        close();
      }else {
        LOGGER.debug("thread recycle at time:{} ",new Date());
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
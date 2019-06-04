package io.mycat.plug.sequence.sequenceMySQL;

import io.mycat.plug.sequence.SequenceCallback;
import io.mycat.plug.sequence.SequenceHandler;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * chenjunwen 2019.6.4
 *
 * 1.本类线程安全 2.异步IO操作,一旦获取多个序列号之后再回调,一次只有一个IO操作运行
 */
public class SequenceMySQLHandler implements SequenceHandler {

  private final AtomicLong currentValue = new AtomicLong(0);//@todo AtomicLongArray
  private final AtomicLong maxSequenceValue = new AtomicLong(-1);
  private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

  @Override
  public void nextId(SequenceCallback callback) {
    boolean successFetched = false;
    long curSeq;
    synchronized (this) {
      successFetched = isSuccessFetched();
      {//消除cas
        long value = currentValue.get() + 1;
        currentValue.set(value);
        curSeq = value;
      }
    }
    if (successFetched) {
      if (curSeq < maxSequenceValue.get()) {
        callback.onSequence(curSeq);
        return;
      }
    }
    fetch(callback);
  }


  private boolean isSuccessFetched() {
    return maxSequenceValue.get() > 0;
  }

  private void fetch(SequenceCallback callback) {
    boolean fetching = maxSequenceValue.getAndSet(-1) == -1;
    if (fetching) {
      queue.add(() -> {
        nextId(callback);
      });
      return;
    }
    pengding(() -> {
      boolean success = false;
      long currentValue = 0;
      long maxSequenceValue = 0;
      Exception exception = null;
      if (success) {
        synchronized (this) {
          this.currentValue.set(currentValue + 1);
          this.maxSequenceValue.set(maxSequenceValue);
        }
        callback.onSequence(currentValue);
      } else {
        callback.onException(exception, this, null);
      }
      pengding(() -> {
        for (Runnable runnable : queue) {
          pengding(runnable);
        }
      });
    });
  }

  private void pengding(Runnable runnable) {

  }
}
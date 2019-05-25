package io.mycat.proxy.callback;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jamie12221
 *  date 2019-05-22 11:50
 **/
public class AsyncTaskCallBackCounter {

  final AtomicInteger counter;
  final AsyncTaskCallBack callBack;
  final AtomicInteger fail = new AtomicInteger(0);

  public AsyncTaskCallBackCounter(int counter,
      AsyncTaskCallBack callBack) {
    this.counter = new AtomicInteger(counter);
    this.callBack = callBack;
  }

  public void onCountSuccess() {
    if (counter.decrementAndGet() == 0) {
      callBack.onFinished(this, null, null);
    }
  }

  public void onCountFail() {
    fail.incrementAndGet();
    if (counter.decrementAndGet() == 0) {
      callBack.onFinished(this, null, null);
    }
  }

  @Override
  public String toString() {
    return "AsyncTaskCallBackCounter{" +
               "counter=" + counter +
               ", callBack=" + callBack +
               ", fail=" + fail +
               '}';
  }
}

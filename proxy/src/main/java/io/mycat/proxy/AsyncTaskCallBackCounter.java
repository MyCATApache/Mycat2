package io.mycat.proxy;

import io.mycat.proxy.session.Session;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jamie12221
 * @date 2019-05-15 10:11
 **/
public final class AsyncTaskCallBackCounter implements AsyncTaskCallBack {

  final AtomicInteger counter;
  final AsyncTaskCallBack callBack;
  final ArrayList errorList = new ArrayList(0);

  public AsyncTaskCallBackCounter(int size, AsyncTaskCallBack callBack) {
    counter = new AtomicInteger(size);
    this.callBack = callBack;
  }

  @Override
  public void finished(Session session, Object sender, boolean success, Object result,
      Object attr) {
    if (!success) {
      errorList.add(result);
    }
    if (counter.decrementAndGet() == 0) {
      callBack.finished(null, this, errorList.isEmpty(), errorList, null);
    }
  }
}

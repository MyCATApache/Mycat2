package io.mycat.proxy.callback;

public enum EmptyAsyncTaskCallBack implements AsyncTaskCallBack {
  INSTANCE;

  @Override
  public void onFinished(Object sender, Object result, Object attr) {

  }

  @Override
  public void onException(Exception e, Object sender, Object attr) {

  }
}
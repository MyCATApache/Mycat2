package io.mycat.proxy.callback;

public interface SessionCallBack<T> extends TaskCallBack<SessionCallBack> {

  void onSession(T session, Object sender, Object attr);

  void onException(Exception exception, Object sender, Object attr);
}
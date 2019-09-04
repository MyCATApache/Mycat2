package io.mycat.bindThread;

public interface BindThreadCallback<KEY extends BindThreadKey, PROCESS extends BindThread> {

  void accept(KEY key, PROCESS context);

  void onException(KEY key, Exception e);
}
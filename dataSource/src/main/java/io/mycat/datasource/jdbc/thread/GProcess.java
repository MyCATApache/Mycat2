package io.mycat.datasource.jdbc.thread;

import io.mycat.bindThread.BindThreadCallback;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.datasource.jdbc.datasource.TransactionSession;

public abstract class GProcess<T extends BindThreadKey> implements BindThreadCallback<T, GThread> {

  @Override
  public void accept(T key, GThread context) {
    accept(key, context.transactionSession);
  }

  public abstract void accept(T key, TransactionSession session);
}
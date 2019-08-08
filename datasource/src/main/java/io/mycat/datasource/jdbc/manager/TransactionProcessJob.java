package io.mycat.datasource.jdbc.manager;

import io.mycat.datasource.jdbc.session.TransactionSession;

public interface TransactionProcessJob {

  void accept(TransactionProcessKey key, TransactionSession session);

  void onException(TransactionProcessKey key, Exception e);
}
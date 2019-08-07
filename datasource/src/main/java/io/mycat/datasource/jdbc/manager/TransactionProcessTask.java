package io.mycat.datasource.jdbc.manager;

import io.mycat.datasource.jdbc.session.TransactionSession;

public interface TransactionProcessTask {

  void accept(TransactionProcessKey key, TransactionSession session);

}
package io.mycat.datasource.jdbc.session;

import io.mycat.CloseableObject;
import io.mycat.datasource.jdbc.transaction.TransactionStatus;

public interface TransactionSession extends TransactionStatus, CloseableObject {

  void commit();

  void rollback();

  boolean isInTransaction();

}
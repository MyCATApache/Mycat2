package io.mycat.datasource.jdbc.connection;

import io.mycat.CloseableObject;
import io.mycat.datasource.jdbc.manager.TransactionStatus;

public interface TransactionConnection extends TransactionStatus, CloseableObject {

  void commit();

  void rollback();

  boolean isInTransaction();

}
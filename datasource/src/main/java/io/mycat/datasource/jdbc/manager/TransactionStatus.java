package io.mycat.datasource.jdbc.manager;

import io.mycat.CloseableObject;

public interface TransactionStatus extends CloseableObject {

  boolean isInTransaction();

  default void tryClose() {
    if (!isInTransaction()) {
      close();
    }
  }
}
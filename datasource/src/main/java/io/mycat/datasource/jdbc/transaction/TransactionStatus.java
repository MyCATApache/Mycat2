package io.mycat.datasource.jdbc.transaction;

import io.mycat.CloseableObject;

public interface TransactionStatus extends CloseableObject {

  boolean isInTransaction();

  default void tryClose() {
    if (!isInTransaction()) {
      close();
    }
  }
}
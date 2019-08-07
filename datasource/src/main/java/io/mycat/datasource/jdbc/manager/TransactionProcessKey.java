package io.mycat.datasource.jdbc.manager;

public abstract class TransactionProcessKey {

  public abstract int hashCode();

  public abstract boolean equals(Object obj);
}
package io.mycat.expression;

public interface RowProvider {

  RowBaseIterator createBatchRow(String key, byte[] command);
}
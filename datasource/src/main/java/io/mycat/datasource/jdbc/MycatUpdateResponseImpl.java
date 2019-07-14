package io.mycat.datasource.jdbc;

import java.io.IOException;

public class MycatUpdateResponseImpl implements MycatUpdateResponse {

  final int updateCount;
  final long lastInsertId;

  public MycatUpdateResponseImpl(int updateCount, long lastInsertId) {
    this.updateCount = updateCount;
    this.lastInsertId = lastInsertId;
  }

  @Override
  public int getUpdateCount() {
    return updateCount;
  }

  @Override
  public long getLastInsertId() {
    return lastInsertId;
  }

  @Override
  public void close() throws IOException {

  }
}
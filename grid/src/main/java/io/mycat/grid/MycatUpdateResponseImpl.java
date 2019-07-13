package io.mycat.grid;

public class MycatUpdateResponseImpl implements MycatUpdateResponse {

  final long updateCount;
  final long lastInsertId;

  public MycatUpdateResponseImpl(long updateCount, long lastInsertId) {
    this.updateCount = updateCount;
    this.lastInsertId = lastInsertId;
  }

  @Override
  public long getUpdateCount() {
    return updateCount;
  }

  @Override
  public long getLastInsertId() {
    return lastInsertId;
  }
}
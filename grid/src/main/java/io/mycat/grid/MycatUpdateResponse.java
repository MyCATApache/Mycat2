package io.mycat.grid;

public interface MycatUpdateResponse extends MycatResponse {

  default MycatResultSetType getType() {
    return MycatResultSetType.UPDATEOK;
  }

  long getUpdateCount();

  long getLastInsertId();
}
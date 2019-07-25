package io.mycat.beans.resultset;

public interface MycatUpdateResponse extends MycatResponse {

  default MycatResultSetType getType() {
    return MycatResultSetType.UPDATEOK;
  }

  int getUpdateCount();

  long getLastInsertId();

  int serverStatus();
}
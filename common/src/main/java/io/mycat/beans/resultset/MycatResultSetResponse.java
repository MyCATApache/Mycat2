package io.mycat.beans.resultset;

import java.util.Iterator;

public interface MycatResultSetResponse extends MycatResponse {

  default MycatResultSetType getType() {
    return MycatResultSetType.RRESULTSET;
  }

  int columnCount();

  Iterator<byte[]> columnDefIterator();

  Iterator<byte[][]> rowIterator();

}

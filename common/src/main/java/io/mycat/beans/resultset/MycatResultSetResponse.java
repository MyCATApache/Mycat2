package io.mycat.beans.resultset;

import java.util.Iterator;

public interface MycatResultSetResponse<T> extends MycatResponse {

  default MycatResultSetType getType() {
    return MycatResultSetType.RRESULTSET;
  }

  int columnCount();

  Iterator<T> columnDefIterator();

  Iterator<T> rowIterator();

}

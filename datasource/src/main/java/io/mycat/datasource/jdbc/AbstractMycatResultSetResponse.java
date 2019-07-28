package io.mycat.datasource.jdbc;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.mysqlapi.collector.RowBaseIterator;
import io.mycat.proxy.MySQLPacketUtil;
import java.io.IOException;
import java.util.Iterator;

public abstract class AbstractMycatResultSetResponse implements MycatResultSetResponse {

  protected final RowBaseIterator iterator;

  public AbstractMycatResultSetResponse(RowBaseIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public int columnCount() {
    return iterator.metaData().getColumnCount();
  }

  @Override
  public Iterator<byte[]> columnDefIterator() {
    return new Iterator<byte[]>() {
      final int count = columnCount();
      int index = 1;

      @Override
      public boolean hasNext() {
        return index <= count;
      }

      @Override
      public byte[] next() {
        return MySQLPacketUtil
            .generateColumnDefPayload(
                iterator.metaData(),
                index++);
      }
    };
  }

  @Override
  public void close() throws IOException {
    iterator.close();
  }
}
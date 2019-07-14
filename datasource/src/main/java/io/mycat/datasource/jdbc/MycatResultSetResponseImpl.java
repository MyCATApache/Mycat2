package io.mycat.datasource.jdbc;

import io.mycat.compute.RowBaseIterator;
import io.mycat.proxy.MySQLPacketUtil;
import java.io.IOException;
import java.util.Iterator;

public class MycatResultSetResponseImpl implements MycatResultSetResponse {

  final RowBaseIterator rowBaseIterator;
  private JdbcSession jdbcSession;

  public MycatResultSetResponseImpl(JdbcSession jdbcSession,
      RowBaseIterator rowBaseIterator) {
    this.jdbcSession = jdbcSession;
    this.rowBaseIterator = rowBaseIterator;
  }

  @Override
  public int columnCount() {
    return rowBaseIterator.metaData().getColumnCount();
  }

  @Override
  public Iterator<byte[]> columnDefIterator() {
    return new Iterator<byte[]>() {
      final int count = MycatResultSetResponseImpl.this.columnCount();
      int index = 1;

      @Override
      public boolean hasNext() {
        return index <= count;
      }

      @Override
      public byte[] next() {
        return MySQLPacketUtil
            .generateColumnDefPayload(MycatResultSetResponseImpl.this.rowBaseIterator.metaData(),
                index++);
      }
    };
  }

  @Override
  public Iterator<byte[][]> rowIterator() {
    return new Iterator<byte[][]>() {
      final int count = MycatResultSetResponseImpl.this.columnCount();

      @Override
      public boolean hasNext() {
        return rowBaseIterator.next();
      }

      @Override
      public byte[][] next() {
        byte[][] bytes = new byte[count][];
        RowBaseIterator rowBaseIterator = MycatResultSetResponseImpl.this.rowBaseIterator;
        for (int i = 0, j = 1; i < count; i++, j++) {
          bytes[i] = rowBaseIterator.getBytes(j);
        }
        return bytes;
      }
    };
  }

  @Override
  public void close() throws IOException {
    rowBaseIterator.close();
    jdbcSession.close(false, "close");
  }
}
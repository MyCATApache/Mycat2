package io.mycat.beans.resultset;

import java.io.IOException;
import java.util.Iterator;

public interface MycatResultSet extends MycatResultSetResponse<byte[]>{

  void addColumnDef(int index, String database, String table,
      String originalTable,
      String columnName, String orgName, int type,
      int columnFlags,
      int columnDecimals, int length);

  void addColumnDef(int index, String columnName, int type);

  int columnCount();

  Iterator<byte[]> columnDefIterator();

  void addTextRowPayload(String... row);

  void addTextRowPayload(byte[]... row);

  void addObjectRowPayload(Object[]... row);

  Iterator<byte[]> rowIterator();

  void close() throws IOException;
}
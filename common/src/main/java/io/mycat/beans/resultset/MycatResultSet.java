package io.mycat.beans.resultset;

import java.io.IOException;
import java.util.Iterator;

public interface MycatResultSet extends MycatResultSetResponse{
  public void addColumnDef(int index,String database, String table,
      String originalTable,
      String columnName, String orgName, int type,
      int columnFlags,
      int columnDecimals, int length);
  public void addColumnDef(int index, String columnName, int type);
  public int columnCount();
  public Iterator<byte[]> columnDefIterator();
  public void addTextRowPayload(String... row);
  public void addTextRowPayload(byte[]... row);
  public void addObjectRowPayload(Object[]...row);
  public Iterator<byte[][]> rowIterator();
  public void close() throws IOException;
}
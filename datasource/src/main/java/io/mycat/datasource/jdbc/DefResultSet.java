package io.mycat.datasource.jdbc;

import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class DefResultSet implements MycatResultSetResponse {

  private final MycatSession session;
  private final byte[][] columnDefList;
  private final byte[][][] rowList;

  public DefResultSet(MycatSession session, int columnCount, int rowCount) {
    this.session = session;
    this.columnDefList = new byte[columnCount][];
    this.rowList = new byte[rowCount][][];
  }

  public void addColumnDef(MycatSession session, int index, String columnName, int type) {
    columnDefList[index] = MySQLPacketUtil
        .generateColumnDefPayload(columnName, type, session.charsetIndex(), session.charset());
  }

  public void addRowPacket(int index,byte[]... row) {
    rowList[index] = row;
  }

  @Override
  public int columnCount() {
    return columnDefList.length;
  }

  @Override
  public Iterator<byte[]> columnDefIterator() {
    return Arrays.asList(columnDefList).iterator();
  }

  @Override
  public Iterator<byte[][]> rowIterator() {
    return Arrays.asList(rowList).iterator();
  }

  @Override
  public void close() throws IOException {

  }
}
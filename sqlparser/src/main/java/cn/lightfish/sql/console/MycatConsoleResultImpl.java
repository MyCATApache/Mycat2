package cn.lightfish.sql.console;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MycatConsoleResultImpl implements MycatConsoleResult {

  final List<String> columnList;
  final List<Object[]> rowList;

  public MycatConsoleResultImpl(int columnCount) {
    this(columnCount, -1);
  }

  public MycatConsoleResultImpl(int columnCount, int expectRowCount) {
    this.columnList = new ArrayList<>(columnCount);
    this.rowList = expectRowCount < 0 ? new ArrayList<>() : new ArrayList<>(expectRowCount);
  }


  @Override
  public List<String> columnDefList() {
    return columnList;
  }

  @Override
  public Iterator<Object[]> rowIterator() {
    return rowList.iterator();
  }

  public void addColumn(String columnName) {
    columnList.add(columnName);
  }

  public void addRow(Object... value) {
    rowList.add(value);
  }
}
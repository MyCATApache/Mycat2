package io.mycat.sqlparser.util;

public class ColumnRef implements SObject {

 final String columnName;

  public ColumnRef(String columnName) {
    this.columnName = columnName;
  }

  @Override
  public String sql() {
    return columnName;
  }
}
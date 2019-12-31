package io.mycat.sqlEngine.schema;

public class TableColumnDefinition extends BaseColumnDefinition {

  DbTable table;

  public TableColumnDefinition(String columnName, Class type) {
    super(columnName, type);
  }

  public DbTable getTable() {
    return table;
  }

  void setTable(DbTable table) {
    this.table = table;
  }
}
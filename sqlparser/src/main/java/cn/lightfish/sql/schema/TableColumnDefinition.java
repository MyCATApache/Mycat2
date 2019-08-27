package cn.lightfish.sql.schema;

public class TableColumnDefinition extends SimpleColumnDefinition {

  MycatTable table;

  public TableColumnDefinition(String columnName, Class type) {
    super(columnName, type);
  }

  public MycatTable getTable() {
    return table;
  }

  public void setTable(MycatTable table) {
    this.table = table;
  }
}
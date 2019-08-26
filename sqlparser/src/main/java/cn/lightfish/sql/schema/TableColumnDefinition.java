package cn.lightfish.sql.schema;

public class TableColumnDefinition extends SimpleColumnDefinition {

  final MycatTable table;

  public TableColumnDefinition(String columnName, Class type,
      MycatTable table) {
    super(columnName, type);
    this.table = table;
  }

  public MycatTable getTable() {
    return table;
  }
}
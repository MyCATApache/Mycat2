package cn.lightfish.sql.schema;

public class MycatColumnDefinition {

  final String columnName;
  final Class type;

  public MycatColumnDefinition(String columnName, Class type) {
    this.columnName = columnName;
    this.type = type;
  }

  public String getColumnName() {
    return columnName;
  }

  public Class getType() {
    return type;
  }

  @Override
  public String toString() {
    return "MycatColumnDefinition{" +
        "columnName='" + columnName + '\'' +
        ", type=" + type +
        '}';
  }
}
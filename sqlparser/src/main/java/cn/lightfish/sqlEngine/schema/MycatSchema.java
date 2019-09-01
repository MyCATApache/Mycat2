package cn.lightfish.sqlEngine.schema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MycatSchema {

  private final String databaseName;
  private final Map<String,MycatTable> tables = new ConcurrentHashMap<>();

  public String getDatabaseName() {
    return databaseName;
  }

  public MycatSchema(String databaseName) {
    this.databaseName = databaseName;
  }

  public MycatTable getTableByName(String tableName) {
    return tables.get(tableName);
  }

  public void createTable(MycatTable table) {
    tables.put(table.tableName,table);
  }

  public void dropTable(String tableName) {
    tables.remove(tableName);
  }

  public void dropTable(List<String> nameList) {
    nameList.forEach(s->tables.remove(s));
  }
}
package io.mycat.sqlEngine.schema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DbSchema {

  private final String databaseName;
  private final Map<String, DbTable> tables = new ConcurrentHashMap<>();

  public String getDatabaseName() {
    return databaseName;
  }

  public DbSchema(String databaseName) {
    this.databaseName = databaseName;
  }

  public DbTable getTableByName(String tableName) {
    return tables.get(tableName);
  }

  public void createTable(DbTable table) {
    tables.put(table.tableName,table);
  }

  public void dropTable(String tableName) {
    tables.remove(tableName);
  }

  public void dropTable(List<String> nameList) {
    nameList.forEach(s->tables.remove(s));
  }
}
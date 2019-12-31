package io.mycat.sqlEngine.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class DbTable implements DbSchemaObject {

  final DbSchema schema;
  final String tableName;
  final TableColumnDefinition[] columnDefinitions;
  final Map<String, TableColumnDefinition> columnDefinitionMap = new HashMap<>();
  final DbPartition mycatPartition;
  private final boolean broadCast;

  public DbTable(DbSchema schema, String tableName,
                 List<TableColumnDefinition> columnDefinitions,
                 DbPartition mycatPartition) {
    this.schema = schema;
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions.toArray(new TableColumnDefinition[]{});
    this.mycatPartition = mycatPartition;
    this.broadCast = false;
    computeTableColumnMap(columnDefinitions);
  }

  public DbTable(DbSchema schema, String tableName,
                 List<TableColumnDefinition> columnDefinitions,
                 boolean broadCast) {
    this.schema = schema;
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions.toArray(new TableColumnDefinition[]{});
    this.broadCast = broadCast;
    this.mycatPartition = null;
    computeTableColumnMap(columnDefinitions);
  }

  public DbPartition getPartition() {
    return mycatPartition;
  }

  public boolean isBroadCast() {
    return broadCast;
  }

  public DbSchema getSchema() {
    return schema;
  }


  public String getTableName() {
    return tableName;
  }

  public TableColumnDefinition[] getColumnDefinitions() {
    return columnDefinitions;
  }

  public DbPartition getMycatPartition() {
    return mycatPartition;
  }

  public TableColumnDefinition getColumnByName(String name) {
    return columnDefinitionMap.get(name);
  }

  private void computeTableColumnMap(List<TableColumnDefinition> columnDefinitions) {
    for (TableColumnDefinition columnDefinition : columnDefinitions) {
      columnDefinitionMap.put(columnDefinition.getColumnName(), columnDefinition);
      columnDefinition.setTable(this);
    }
  }
}
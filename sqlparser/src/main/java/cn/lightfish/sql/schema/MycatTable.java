package cn.lightfish.sql.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class MycatTable implements MycatSchemaObject {

  final MycatSchema schema;
  final String tableName;
  final TableColumnDefinition[] columnDefinitions;
  final Map<String, TableColumnDefinition> columnDefinitionMap = new HashMap<>();
  final MycatPartition mycatPartition;
  private final boolean broadCast;

  public MycatTable(MycatSchema schema, String tableName,
      List<TableColumnDefinition> columnDefinitions,
      MycatPartition mycatPartition) {
    this.schema = schema;
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions.toArray(new TableColumnDefinition[]{});
    this.mycatPartition = mycatPartition;
    this.broadCast = false;
    computeTableColumnMap(columnDefinitions);
  }

  public MycatTable(MycatSchema schema, String tableName,
      List<TableColumnDefinition> columnDefinitions,
      boolean broadCast) {
    this.schema = schema;
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions.toArray(new TableColumnDefinition[]{});
    this.broadCast = broadCast;
    this.mycatPartition = null;
    computeTableColumnMap(columnDefinitions);
  }

  public MycatPartition getPartition() {
    return mycatPartition;
  }

  public boolean isBroadCast() {
    return broadCast;
  }

  public MycatSchema getSchema() {
    return schema;
  }


  public String getTableName() {
    return tableName;
  }

  public TableColumnDefinition[] getColumnDefinitions() {
    return columnDefinitions;
  }

  public MycatPartition getMycatPartition() {
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
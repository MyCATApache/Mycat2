package cn.lightfish.sql.schema;

import java.util.List;

public class MycatTable implements MycatSchemaObject {

  final String tableName;
  final List<MycatColumnDefinition> columnDefinitions;

  MycatPartition mycatPartition;
  private final boolean broadCast;

  public MycatTable(String tableName, List<MycatColumnDefinition> columnDefinitions,
      MycatPartition mycatPartition) {
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions;
    this.mycatPartition = mycatPartition;
    this.broadCast = false;
  }


  public MycatTable(String tableName, List<MycatColumnDefinition> columnDefinitions,
      boolean broadCast) {
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions;
    this.broadCast = broadCast;
  }

  public MycatPartition getPartition() {
    return mycatPartition;
  }
}
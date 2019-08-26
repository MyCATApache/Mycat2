package cn.lightfish.sql.schema;

import java.util.List;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class MycatTable implements MycatSchemaObject {

  final String tableName;
  final List<SimpleColumnDefinition> columnDefinitions;

  MycatPartition mycatPartition;
  private final boolean broadCast;

  public MycatTable(String tableName, List<SimpleColumnDefinition> columnDefinitions,
      MycatPartition mycatPartition) {
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions;
    this.mycatPartition = mycatPartition;
    this.broadCast = false;
  }


  public MycatTable(String tableName, List<SimpleColumnDefinition> columnDefinitions,
      boolean broadCast) {
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions;
    this.broadCast = broadCast;
  }

  public MycatPartition getPartition() {
    return mycatPartition;
  }

  public boolean isBroadCast() {
    return broadCast;
  }
}
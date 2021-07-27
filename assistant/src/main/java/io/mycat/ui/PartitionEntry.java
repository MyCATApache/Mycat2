package io.mycat.ui;

import io.mycat.BackendTableInfo;
import io.mycat.Partition;
import lombok.Data;


@Data
public class PartitionEntry {
    public int globalIndex;
    public int dbIndex;
    public int tableIndex;
    public String target;
    public String schema;
    public String table;

    public static PartitionEntry  of(
            int index,
            int dbIndex,
             int tableIndex,
             String target,
             String schema,
             String table
    ){
        PartitionEntry partitionEntry = new PartitionEntry();
        partitionEntry.setGlobalIndex(index);
        partitionEntry.setDbIndex(dbIndex);
        partitionEntry.setTableIndex(tableIndex);
        partitionEntry.setSchema(schema);
        partitionEntry.setTable(table);
        partitionEntry.setTarget(target);
        return partitionEntry;
    }
    public static PartitionEntry  of(
            int index,
            Partition partition
    ){
     return of(index,0,0,partition.getTargetName(),partition.getSchema(),partition.getTable());
    }

    public Partition toPartition(){
        return new BackendTableInfo(getTarget(),getSchema(),getTable());
    }
}

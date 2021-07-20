package io.mycat.ui;

import io.mycat.Partition;
import lombok.Data;


@Data
public class PartitionEntry {
    public String target;
    public String schema;
    public String table;

    public static PartitionEntry  of(
             String target,
             String schema,
             String table
    ){
        PartitionEntry partitionEntry = new PartitionEntry();
        partitionEntry.setSchema(schema);
        partitionEntry.setTable(table);
        partitionEntry.setTarget(target);
        return partitionEntry;
    }
    public static PartitionEntry  of(
            Partition partition
    ){
     return of(partition.getTargetName(),partition.getSchema(),partition.getTable());
    }
}

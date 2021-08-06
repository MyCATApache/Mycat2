package io.mycat.ui;

import io.mycat.BackendTableInfo;
import io.mycat.IndexBackendTableInfo;
import io.mycat.Partition;
import lombok.Data;


@Data
public class PartitionEntry {
    public String globalIndex;
    public String dbIndex;
    public String tableIndex;
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
        partitionEntry.setGlobalIndex(String.valueOf(index));
        partitionEntry.setDbIndex(String.valueOf(dbIndex));
        partitionEntry.setTableIndex(String.valueOf(tableIndex));
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
        return new IndexBackendTableInfo(getTarget(),getSchema(),getTable(),
                Integer.parseInt(getDbIndex()),
                Integer.parseInt(  getTableIndex()),
                Integer.parseInt(getGlobalIndex()));
    }
}

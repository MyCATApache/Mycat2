package io.mycat.calcite.table;

import io.mycat.Partition;
import io.mycat.SimpleColumnInfo;
import io.mycat.router.CustomRuleFunction;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
public class ShardingIndexTable extends ShardingTable {
    private final ShardingTable primaryTable;
    private final  String indexName;
    public ShardingIndexTable(String indexName,LogicTable logicTable,
                              List<Partition> backends,
                              CustomRuleFunction shardingFuntion,
                              ShardingTable primaryTable) {
        super(logicTable, backends, shardingFuntion, Collections.emptyList());
        this.indexName = indexName;
        this.primaryTable = primaryTable;
    }

    public ShardingIndexTable withPrimary(ShardingTable shardingTable){
        return new ShardingIndexTable(getIndexName(),getLogicTable(),getBackends(),getShardingFuntion(),shardingTable);
    }

    public boolean hasFactColumn(int i) {
        ShardingTable factTable = getFactTable();
        SimpleColumnInfo column = this.getColumnByName(factTable.getColumns().get(i).getColumnName());
        return column!=null;
    }

    public int mappingIndexTableIndex(int i) {
        ShardingTable factTable = getFactTable();
        SimpleColumnInfo column = this.getColumnByName(factTable.getColumns().get(i).getColumnName());
        return Objects.requireNonNull(column).getId();
    }

    public ShardingTable getFactTable() {
        return primaryTable;
    }
}

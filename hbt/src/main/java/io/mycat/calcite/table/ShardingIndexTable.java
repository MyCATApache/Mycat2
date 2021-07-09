package io.mycat.calcite.table;

import io.mycat.Partition;
import io.mycat.SimpleColumnInfo;
import io.mycat.router.CustomRuleFunction;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ShardingIndexTable extends ShardingTable {
    private final ShardingTable primaryTable;

    public ShardingIndexTable(LogicTable logicTable,
                              List<Partition> backends,
                              CustomRuleFunction shardingFuntion,
                              ShardingTable primaryTable) {
        super(logicTable, backends, shardingFuntion, Collections.emptyList());
        this.primaryTable = primaryTable;
    }

    public ShardingIndexTable withPrimary(ShardingTable shardingTable){
        return new ShardingIndexTable(getLogicTable(),getBackends(),getShardingFuntion(),shardingTable);
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

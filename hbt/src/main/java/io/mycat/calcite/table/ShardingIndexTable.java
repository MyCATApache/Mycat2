package io.mycat.calcite.table;

import io.mycat.Partition;
import io.mycat.SimpleColumnInfo;
import io.mycat.router.CustomRuleFunction;

import java.util.List;

public class ShardingIndexTable extends ShardingTable {
    private final ShardingTable primaryTable;

    public ShardingIndexTable(LogicTable logicTable, List<Partition> backends, CustomRuleFunction shardingFuntion, ShardingTable primaryTable) {
        super(logicTable, backends, shardingFuntion);
        this.primaryTable = primaryTable;
    }

    public boolean hasFactColumn(int i) {
        ShardingTable factTable = getFactTable();
        SimpleColumnInfo column = this.getColumnByName(factTable.getColumns().get(i).getColumnName());
        return column!=null;
    }

    public int mappingIndexTableIndex(int i) {
        ShardingTable factTable = getFactTable();
        SimpleColumnInfo column = this.getColumnByName(factTable.getColumns().get(i).getColumnName());
        return column.getId();
    }

    public ShardingTable getFactTable() {
        return primaryTable;
    }
}

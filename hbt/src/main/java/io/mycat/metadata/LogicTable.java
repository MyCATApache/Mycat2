package io.mycat.metadata;

import io.mycat.BackendTableInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.queryCondition.SimpleColumnInfo;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;
import java.util.Map;

@Getter
 public class LogicTable {
    private final LogicTableType type;
    private final String schemaName;
    private final String tableName;
    private final List<SimpleColumnInfo> rawColumns;
    private final String createTableSQL;

    public LogicTable(LogicTableType type,String schemaName,
                      String tableName,
                      List<SimpleColumnInfo> rawColumns,
                      String createTableSQL) {
        /////////////////////////////////////////
        this.type = type;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.rawColumns = rawColumns;
        this.createTableSQL = createTableSQL;
        /////////////////////////////////////////

    }

    public static ShardingTableHandler createShardingTable(String schemaName, String name, List<BackendTableInfo> backends, List<SimpleColumnInfo> rawColumns,
                                                                        Map<SimpleColumnInfo.@NonNull ShardingType, SimpleColumnInfo.ShardingInfo> shardingInfo, String createTableSQL) {
        LogicTable logicTable = new LogicTable(LogicTableType.SHARDING, schemaName, name, rawColumns, createTableSQL);
        return new ShardingTable(logicTable, backends, shardingInfo);
    }

    public static GlobalTableHandler createGlobalTable(String schemaName, String tableName, List<BackendTableInfo> backendTableInfos, List<BackendTableInfo> readOnly, LoadBalanceStrategy loadBalance, List<SimpleColumnInfo> columns, String createTableSQL) {
        LogicTable logicTable = new LogicTable(LogicTableType.GLOBAL, schemaName, tableName, columns, createTableSQL);
        return new GlobalTable(logicTable,backendTableInfos,readOnly,loadBalance);
    }

}

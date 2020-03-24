package io.mycat.metadata;

import io.mycat.BackendTableInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.queryCondition.SimpleColumnInfo;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Getter
public class LogicTable {
    private final String uniqueName;
    private final LogicTableType type;
    private final String schemaName;
    private final String tableName;
    private final List<SimpleColumnInfo> rawColumns;
    private final String createTableSQL;
    private final SimpleColumnInfo autoIncrementColumn;

    //优化,非必须
    private final Map<String, SimpleColumnInfo> map;


    public LogicTable(LogicTableType type, String schemaName,
                      String tableName,
                      List<SimpleColumnInfo> rawColumns,
                      String createTableSQL) {
        /////////////////////////////////////////
        this.uniqueName = schemaName.toLowerCase() + "_" + tableName.toLowerCase();
        this.type = type;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.rawColumns = rawColumns;
        this.createTableSQL = createTableSQL;
        /////////////////////////////////////////
        this.autoIncrementColumn = rawColumns.stream().filter(i -> i.isAutoIncrement()).findFirst().orElse(null);
        /////////////////////////////////////////
        this.map = rawColumns.stream().collect(Collectors.toMap(k -> k.getColumnName(), Function.identity()));
    }

    public static TableHandler createShardingTable(String schemaName,
                                                   String name,
                                                   List<BackendTableInfo> backends, List<SimpleColumnInfo> rawColumns,
                                                   Map<SimpleColumnInfo.@NonNull ShardingType, SimpleColumnInfo.ShardingInfo> shardingInfo,
                                                   String createTableSQL,
                                                  Supplier<String> sequence) {
        LogicTable logicTable = new LogicTable(LogicTableType.SHARDING, schemaName, name, rawColumns, createTableSQL);
        ShardingTable shardingTable = new ShardingTable(logicTable, backends, shardingInfo,sequence);
        return shardingTable;
    }

    public static TableHandler createGlobalTable(String schemaName, String tableName, List<BackendTableInfo> backendTableInfos, List<BackendTableInfo> readOnly, LoadBalanceStrategy loadBalance, List<SimpleColumnInfo> columns, String createTableSQL) {
        LogicTable logicTable = new LogicTable(LogicTableType.GLOBAL, schemaName, tableName, columns, createTableSQL);
        GlobalTable globalTable = new GlobalTable(logicTable, backendTableInfos, readOnly, loadBalance);
        return globalTable;
    }

    public SimpleColumnInfo getColumnByName(String name) {
        return this.map.get(name);
    }

    public String getUniqueName() {
        return uniqueName;
    }
}

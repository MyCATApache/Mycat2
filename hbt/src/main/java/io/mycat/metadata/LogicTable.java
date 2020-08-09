package io.mycat.metadata;

import io.mycat.*;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import lombok.Getter;
import lombok.NonNull;

import java.util.*;
import java.util.function.Supplier;

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
        this.uniqueName = schemaName + "_" + tableName;
        this.type = type;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.rawColumns = rawColumns;
        this.createTableSQL = Objects.requireNonNull(createTableSQL,  this.uniqueName+" createTableSQL is not existed");
        /////////////////////////////////////////
        this.autoIncrementColumn = rawColumns.stream().filter(i -> i.isAutoIncrement()).findFirst().orElse(null);
        /////////////////////////////////////////
        Map<String, SimpleColumnInfo> result = new HashMap<>();
        for (SimpleColumnInfo k : rawColumns) {
            result.put(k.getColumnName(), k);
        }
        this.map = result;
    }

    public static TableHandler createGlobalTable(String schemaName,
                                                 String tableName,
                                                 List<DataNode> backendTableInfos,
                                                 LoadBalanceStrategy loadBalance,
                                                 List<SimpleColumnInfo> columns,
                                                 String createTableSQL) {
        LogicTable logicTable = new LogicTable(LogicTableType.GLOBAL, schemaName, tableName, columns, createTableSQL);
        return new GlobalTable(logicTable, backendTableInfos,loadBalance);
    }

    public static TableHandler createNormalTable(String schemaName,
                                                 String tableName,
                                                 DataNode dataNode,
                                                 List<SimpleColumnInfo> columns,
                                                 String createTableSQL) {
        LogicTable logicTable = new LogicTable(LogicTableType.NORMAL, schemaName, tableName, columns, createTableSQL);
        return new NormalTable(logicTable, dataNode);
    }

    public SimpleColumnInfo getColumnByName(String name) {
        SimpleColumnInfo simpleColumnInfo = this.map.get(name);
        if (simpleColumnInfo == null) {
            SimpleColumnInfo simpleColumnInfo1 = this.map.get(name.toLowerCase());
            if (simpleColumnInfo1 == null) {
                return this.map.get(name.toUpperCase());
            } else {
                return simpleColumnInfo1;
            }
        } else {
            return simpleColumnInfo;
        }
    }

    public String getUniqueName() {
        return uniqueName;
    }
}

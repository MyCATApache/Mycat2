package io.mycat.calcite.metadata;

import io.mycat.BackendTableInfo;
import lombok.Getter;
import lombok.NonNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.mycat.calcite.metadata.SimpleColumnInfo.ShardingType.*;

@Getter
public class LogicTable {
    private final String schemaName;
    private final String tableName;
    private final List<BackendTableInfo> backends;
    private final List<SimpleColumnInfo> rawColumns;
    private final String createTableSQL;
    //////////////optional/////////////////
//        private JdbcTable jdbcTable;
    //////////////optional/////////////////
    private final SimpleColumnInfo.ShardingInfo natureTableColumnInfo;
    private final SimpleColumnInfo.ShardingInfo replicaColumnInfo;
    private final SimpleColumnInfo.ShardingInfo databaseColumnInfo;
    private final SimpleColumnInfo.ShardingInfo tableColumnInfo;


    public LogicTable(String schemaName, String name, List<BackendTableInfo> backends, List<SimpleColumnInfo> rawColumns,
                      Map<SimpleColumnInfo.@NonNull ShardingType, SimpleColumnInfo.ShardingInfo> shardingInfo, String createTableSQL) {
        this.schemaName = schemaName;
        this.tableName = name;
        this.backends = backends == null ? Collections.emptyList() : backends;
        this.rawColumns = rawColumns;
        this.createTableSQL = createTableSQL;


        this.natureTableColumnInfo = shardingInfo.get(NATURE_DATABASE_TABLE);

        this.replicaColumnInfo = shardingInfo.get(MAP_TARGET);
        this.databaseColumnInfo = shardingInfo.get(MAP_DATABASE);
        this.tableColumnInfo = shardingInfo.get(MAP_TABLE);
    }

    public boolean isNatureTable() {
        return natureTableColumnInfo != null;
    }


    public List<BackendTableInfo> getBackends() {
        return backends;
    }
}

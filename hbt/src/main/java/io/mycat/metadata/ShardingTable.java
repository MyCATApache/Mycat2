package io.mycat.metadata;

import io.mycat.BackendTableInfo;
import io.mycat.hbt.TextUpdateInfo;
import io.mycat.queryCondition.SimpleColumnInfo;
import lombok.Getter;
import lombok.NonNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.mycat.queryCondition.SimpleColumnInfo.ShardingType.*;

@Getter
public class ShardingTable implements ShardingTableHandler {
    private final LogicTable logicTable;

    private final SimpleColumnInfo.ShardingInfo natureTableColumnInfo;
    private final SimpleColumnInfo.ShardingInfo replicaColumnInfo;
    private final SimpleColumnInfo.ShardingInfo databaseColumnInfo;
    private final SimpleColumnInfo.ShardingInfo tableColumnInfo;
    private final List<BackendTableInfo> backends;

    public ShardingTable(LogicTable logicTable,
                         List<BackendTableInfo> backends,
                         Map<SimpleColumnInfo.@NonNull ShardingType, SimpleColumnInfo.ShardingInfo> shardingInfo) {
        this.logicTable = logicTable;
        this.backends = backends == null ? Collections.emptyList() : backends;
        this.natureTableColumnInfo = shardingInfo.get(NATURE_DATABASE_TABLE);
        this.replicaColumnInfo = shardingInfo.get(MAP_TARGET);
        this.databaseColumnInfo = shardingInfo.get(MAP_DATABASE);
        this.tableColumnInfo = shardingInfo.get(MAP_TABLE);
    }

    public boolean isNatureTable() {
        return natureTableColumnInfo != null;
    }

    @Override
    public List<BackendTableInfo> getShardingBackends() {
        return backends;
    }

    @Override
    public List<SimpleColumnInfo> getRawColumns() {
        return logicTable.getRawColumns();
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> insertHandler() {
        return new Function<ParseContext, Iterator<TextUpdateInfo>>() {
            @Override
            public Iterator<TextUpdateInfo> apply(ParseContext s) {
                return MetadataManager.routeInsertFlat(getSchemaName(), s.getSql())
                        .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
            }
        };
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> updateHandler() {
        return new Function<ParseContext, Iterator<TextUpdateInfo>>() {
            @Override
            public Iterator<TextUpdateInfo> apply(ParseContext s) {
                return MetadataManager.INSTANCE.rewriteSQL(getSchemaName(),s.getSql())
                        .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
            }
        };
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> deleteHandler() {
        return new Function<ParseContext, Iterator<TextUpdateInfo>>() {
            @Override
            public Iterator<TextUpdateInfo> apply(ParseContext s) {
                return MetadataManager.INSTANCE.rewriteSQL(getSchemaName(),s.getSql())
                        .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
            }
        };
    }

    @Override
    public LogicTableType getType() {
        return LogicTableType.SHARDING;
    }

    @Override
    public String getSchemaName() {
        return logicTable.getSchemaName();
    }

    @Override
    public String getTableName() {
        return logicTable.getTableName();
    }

    @Override
    public String getCreateTableSQL() {
        return logicTable.getCreateTableSQL();
    }
}

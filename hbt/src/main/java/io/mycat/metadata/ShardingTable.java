package io.mycat.metadata;

import io.mycat.*;
import io.mycat.config.ShardingQueryRootConfig;
import io.mycat.config.SharingFuntionRootConfig;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.router.function.PartitionRuleFunctionManager;
import lombok.Getter;
import lombok.NonNull;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.mycat.SimpleColumnInfo.ShardingType.*;

@Getter
public class ShardingTable implements ShardingTableHandler {
    private final LogicTable logicTable;

    private final SimpleColumnInfo.ShardingInfo natureTableColumnInfo;
    private final SimpleColumnInfo.ShardingInfo replicaColumnInfo;
    private final SimpleColumnInfo.ShardingInfo databaseColumnInfo;
    private final SimpleColumnInfo.ShardingInfo tableColumnInfo;
    private final Supplier<String> sequence;
    private final List<DataNode> backends;

    public ShardingTable(LogicTable logicTable,
                         List<DataNode> backends, List<ShardingQueryRootConfig.Column> columns, Supplier<String> sequence) {
        this.logicTable = logicTable;
        this.backends = backends == null ? Collections.emptyList() : backends;
        this.sequence = sequence;

        Map<SimpleColumnInfo.ShardingType, SimpleColumnInfo.ShardingInfo> shardingInfo = getShardingInfo(this, logicTable.getRawColumns(), columns);
        this.natureTableColumnInfo = shardingInfo.get(NATURE_DATABASE_TABLE);
        this.replicaColumnInfo = shardingInfo.get(MAP_TARGET);
        this.databaseColumnInfo = shardingInfo.get(MAP_SCHEMA);
        this.tableColumnInfo = shardingInfo.get(MAP_TABLE);

    }

    private static Map<SimpleColumnInfo.@NonNull ShardingType, SimpleColumnInfo.ShardingInfo> getShardingInfo(
            ShardingTableHandler table,
            List<SimpleColumnInfo> columns,
            List<ShardingQueryRootConfig.Column> columnMap) {
        return columnMap.stream().map(entry1 -> {
            SharingFuntionRootConfig.ShardingFuntion function = entry1.getFunction();
            CustomRuleFunction ruleAlgorithm = PartitionRuleFunctionManager.INSTANCE.
                    getRuleAlgorithm(table, entry1.getColumnName(), function.getClazz(), function.getProperties(), function.getRanges());
            SimpleColumnInfo.ShardingType shardingType = SimpleColumnInfo.ShardingType.parse(entry1.getShardingType());
            SimpleColumnInfo found = null;
            for (SimpleColumnInfo i : columns) {
                if (entry1.getColumnName().equals(i.getColumnName())) {
                    found = i;
                    break;
                }
            }
            SimpleColumnInfo simpleColumnInfo = Objects.requireNonNull(found,table.getSchemaName()+"."+table.getTableName()+" unknown column name:"+entry1.getColumnName());
            return new SimpleColumnInfo.ShardingInfo(simpleColumnInfo, shardingType, entry1.getMap(), ruleAlgorithm);
        }).collect(Collectors.toMap(k -> k.getShardingType() != null ? k.getShardingType() : NATURE_DATABASE_TABLE, k -> k));
    }

    public boolean isNatureTable() {
        return natureTableColumnInfo != null;
    }

    @Override
    public List<DataNode> getShardingBackends() {
        return backends;
    }

    @Override
    public List<SimpleColumnInfo> getColumns() {
        return logicTable.getRawColumns();
    }

    @Override
    public SimpleColumnInfo getColumnByName(String name) {
        return logicTable.getColumnByName(name);
    }

    @Override
    public SimpleColumnInfo getAutoIncrementColumn() {
        return logicTable.getAutoIncrementColumn();
    }

    @Override
    public String getUniqueName() {
        return logicTable.getUniqueName();
    }

    @Override
    public Supplier<String> nextSequence() {
        return sequence;
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> insertHandler() {
        return new Function<ParseContext, Iterator<TextUpdateInfo>>() {
            @Override
            public Iterator<TextUpdateInfo> apply(ParseContext s) {
                List<TextUpdateInfo> collect = MetadataManager.routeInsertFlat(getSchemaName(), s.getSql())
                        .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).collect(Collectors.toList());
                return collect.iterator();
            }
        };
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> updateHandler() {
        return new Function<ParseContext, Iterator<TextUpdateInfo>>() {
            @Override
            public Iterator<TextUpdateInfo> apply(ParseContext s) {
                return MetadataManager.INSTANCE.rewriteSQL(getSchemaName(), s.getSql())
                        .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
            }
        };
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> deleteHandler() {
        return new Function<ParseContext, Iterator<TextUpdateInfo>>() {
            @Override
            public Iterator<TextUpdateInfo> apply(ParseContext s) {
                return MetadataManager.INSTANCE.rewriteSQL(getSchemaName(), s.getSql())
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

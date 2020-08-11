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
    private CustomRuleFunction shardingFuntion;
    private final Supplier<String> sequence;
    private final List<DataNode> backends;

    public ShardingTable(LogicTable logicTable,
                         List<DataNode> backends,
                         CustomRuleFunction shardingFuntion,
                         Supplier<String> sequence) {
        this.logicTable = logicTable;
        this.backends = backends == null ? Collections.emptyList() : backends;
        this.shardingFuntion = shardingFuntion;
        this.sequence = sequence;
    }

    @Override
    public CustomRuleFunction function() {
        return shardingFuntion;
    }

    @Override
    public List<DataNode> dataNodes() {
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
        return s -> {
            List<TextUpdateInfo> collect = MetadataManager.routeInsertFlat(getSchemaName(), s.getSql())
                    .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).collect(Collectors.toList());
            return collect.iterator();
        };
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> updateHandler() {
        return s -> MetadataManager.INSTANCE.rewriteSQL(getSchemaName(), s.getSql())
                .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
    }

    @Override
    public Function<ParseContext, Iterator<TextUpdateInfo>> deleteHandler() {
        return s -> MetadataManager.INSTANCE.rewriteSQL(getSchemaName(), s.getSql())
                .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
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

    public void setShardingFuntion(CustomRuleFunction shardingFuntion) {
        this.shardingFuntion = shardingFuntion;
    }
}

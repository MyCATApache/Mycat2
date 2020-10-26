package io.mycat.metadata;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.*;
import io.mycat.config.ShardingQueryRootConfig;
import io.mycat.config.SharingFuntionRootConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
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
import static io.mycat.metadata.LogicTable.rewriteCreateTableSql;

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
    public void createPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            connection.executeUpdate(getCreateTableSQL(), false);
        }
        for (DataNode node : getBackends()) {
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(node.getTargetName())) {
                connection.executeUpdate(rewriteCreateTableSql(getCreateTableSQL(),node.getSchema(), node.getTable()), false);
            }
        }
    }

    @Override
    public void dropPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        String dropTemplate = "drop table `%s`.`%s`";
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            connection.executeUpdate(String.format(dropTemplate,getSchemaName(),getTableName()), false);
        }
    }

////    @Override
//    public Function<MySqlInsertStatement, Iterable<TextUpdateInfo>> insertHandler() {
//        return s -> {
//            List<TextUpdateInfo> collect = MetadataManager.routeInsertFlat(getSchemaName(), s.toString())
//                    .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).collect(Collectors.toList());
//            return collect;
//        };
//    }
//
////    @Override
//    public Function<MySqlUpdateStatement, Iterable<TextUpdateInfo>> updateHandler() {
//        return (s)-> ()->MetadataManager.INSTANCE.rewriteSQL(getSchemaName(), s.toString())
//                .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
//    }
//
////    @Override
//    public Function<MySqlDeleteStatement, Iterable<TextUpdateInfo>> deleteHandler() {
//        return s ->()-> MetadataManager.INSTANCE.rewriteSQL(getSchemaName(), s.toString())
//                .entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
//    }

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

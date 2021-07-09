/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.gsi.GSIService;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.querycondition.KeyMeta;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.CreateTableUtils;
import lombok.Getter;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.mycat.util.CreateTableUtils.normalizeCreateTableSQLToMySQL;
import static io.mycat.util.DDLHelper.createDatabaseIfNotExist;

@Getter
public class ShardingTable implements ShardingTableHandler {
    private final LogicTable logicTable;
    private CustomRuleFunction shardingFuntion;
    private List<Partition> backends;
    public List<ShardingIndexTable> indexTables;

    public ShardingTable(LogicTable logicTable,
                         List<Partition> backends,
                         CustomRuleFunction shardingFuntion,
                         List<ShardingIndexTable> shardingIndexTables) {
        this.logicTable = logicTable;
        this.backends = (backends == null || backends.isEmpty()) ? Collections.emptyList() : backends;
        this.shardingFuntion = shardingFuntion;
        this.indexTables = shardingIndexTables.stream().map(i->i.withPrimary(ShardingTable.this)).collect(Collectors.toList());
    }

    @Override
    public CustomRuleFunction function() {
        return shardingFuntion;
    }

    @Override
    public List<Partition> dataNodes() {
        return backends;
    }

    @Override
    public List<SimpleColumnInfo> getColumns() {
        return logicTable.getRawColumns();
    }

    @Override
    public Map<String, IndexInfo> getIndexes() {
        return logicTable.getIndexes();
    }

    @Override
    public Optional<Iterable<Object[]>> canIndexTableScan(int[] projects) {
        if (MetaClusterCurrent.exist(GSIService.class)) {
            GSIService gsiService = MetaClusterCurrent.wrapper(GSIService.class);
            return gsiService.scanProject(getSchemaName(), getTableName(), projects);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Iterable<Object[]>> canIndexTableScan(int[] projects, int[] filterIndexes, Object[] values) {
        if (MetaClusterCurrent.exist(GSIService.class)) {
            GSIService gsiService = MetaClusterCurrent.wrapper(GSIService.class);
            return gsiService.scanProjectFilter(getSchemaName(), getTableName(), projects, filterIndexes, values);
        } else {
            return Optional.empty();
        }
    }

//
//    @Override
//    public <T> Optional canIndexTableScan(int[] projects, List<T> nodes) {
//        if (MetaClusterCurrent.exist(GSIService.class)) {
//            GSIService gsiService = MetaClusterCurrent.wrapper(GSIService.class);
//            List<RexNode> rexNodes = (List<RexNode>) nodes;
//            if (rexNodes.size() == 1) {
//                RexNode rexNode = rexNodes.get(0);
//                if (rexNode.getKind() == SqlKind.EQUALS) {
//                    RexCall rexNode1 = (RexCall) rexNode;
//                    List<RexNode> operands = rexNode1.getOperands();
//                    RexNode left = operands.get(0);
//                    left = unCastWrapper(left);
//                    RexNode right = operands.get(1);
//                    right = unCastWrapper(right);
//                    int index = ((RexInputRef) left).getIndex();
//                    Object value = ((RexLiteral) right).getValue2();
//                    return gsiService.scanProjectFilter(index, value);
//                }
//            }
//            return Optional.empty();
//        } else {
//            return Optional.empty();
//        }
//    }

    @Override
    public Optional<Iterable<Object[]>> canIndexTableScan() {
        if (MetaClusterCurrent.exist(GSIService.class)) {
            GSIService gsiService = MetaClusterCurrent.wrapper(GSIService.class);
            return gsiService.scan(getSchemaName(), getTableName());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean canIndex() {
        if (MetaClusterCurrent.exist(GSIService.class)) {
            GSIService gsiService = MetaClusterCurrent.wrapper(GSIService.class);
            return gsiService.isIndexTable(getSchemaName(), getTableName());
        } else {
            return false;
        }
    }

    @Override
    public int getIndexBColumnName(String name) {
        return this.logicTable.getIndexBColumnName(name);
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
    public Supplier<Number> nextSequence() {
        SequenceGenerator sequenceGenerator = MetaClusterCurrent.wrapper(SequenceGenerator.class);
        return sequenceGenerator.getSequence(getUniqueName());
    }

    @Override
    public void createPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        List<Partition> partitions = (List) ImmutableList.builder().add((Partition) (new BackendTableInfo("prototype", getSchemaName(), getTableName()))).addAll(getBackends()).build();
        partitions.stream().parallel().forEach(node -> CreateTableUtils.createPhysicalTable(jdbcConnectionManager, node, getCreateTableSQL()));
    }

    @Override
    public void dropPhysicalTables() {

        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        String dropTemplate = "drop table `%s`.`%s`";
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            connection.executeUpdate(String.format(dropTemplate, getSchemaName(), getTableName()), false);
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
        if (this.backends == null || this.backends.isEmpty()) {
            this.backends = shardingFuntion.calculate(Collections.emptyMap());
        }
    }

    public List<KeyMeta> keyMetas() {
        List<SimpleColumnInfo> shardingKeys = this.getColumns().stream().filter(i -> i.isShardingKey()).collect(Collectors.toList());
        List<KeyMeta> keyMetas = new ArrayList<>();
        for (int i = 0; i < shardingKeys.size(); i++) {
            keyMetas.add(KeyMeta.of("" + i, shardingKeys.get(i).getColumnName()));
        }
        return keyMetas;
    }
}

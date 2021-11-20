/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.google.common.collect.ImmutableList;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.table.*;
import io.mycat.config.*;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.router.function.IndexDataNode;
import io.mycat.router.mycat1xfunction.PartitionRuleFunctionManager;
import io.mycat.util.NameMap;
import io.mycat.util.Pair;
import io.mycat.util.SplitUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Junwen Chen
 **/
public class MetadataManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);
    protected final NameMap<SchemaHandler> schemaMap = new NameMap<>();
    @Getter
    public final static String prototype = PrototypeService.PROTOTYPE;

    @Getter
    private final Map<String, List<ShardingTable>> erTableGroup = new HashMap<>();
    private final Map<String, Map<String, Partition>> targetTableGroup = new HashMap<>();
    private final PrototypeService prototypeService;


    public void removeSchema(String schemaName) {
        schemaMap.remove(schemaName);
    }

    public void addTable(String schemaName,
                         String tableName,
                         ShardingTableConfig tableConfig,
                         ShardingBackEndTableInfoConfig backends,
                         List<ShardingIndexTable> indexTables) {
        addShardingTable(schemaName, tableName, tableConfig, getBackendTableInfos(backends), indexTables);
    }

    public void addProcedure(String schemaNameArg, String procedureNameArg, NormalProcedureConfig config) {
        String schemaName = SQLUtils.normalize(schemaNameArg);
        String procedureName = SQLUtils.normalize(procedureNameArg);
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler != null) {
            NameMap<ProcedureHandler> procedures = schemaHandler.procedures();

            Optional.ofNullable(config.getLocality()).ifPresent(c -> {
                if (c.getSchemaName() == null) {
                    c.setSchemaName(schemaName);
                }
                if (c.getProcedureName() == null) {
                    c.setProcedureName(procedureName);
                }
                if (c.getTargetName() == null) {
                    c.setTargetName(getPrototype());
                }
            });
            procedures.put(procedureName, new NormalProcedureHandler(procedureName, config));
        }
    }

    public void removeProcedure(String schemaName, String procedureName) {
        schemaName = SQLUtils.normalize(schemaName);
        procedureName = SQLUtils.normalize(procedureName);
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler != null) {
            NameMap<ProcedureHandler> procedures = schemaHandler.procedures();
            procedures.remove(procedureName);
        }
    }

    public Optional<ProcedureHandler> getProcedure(String schemaName, String procedureName) {
        schemaName = SQLUtils.normalize(schemaName);
        procedureName = SQLUtils.normalize(procedureName);
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler != null) {
            NameMap<ProcedureHandler> procedures = schemaHandler.procedures();
            ProcedureHandler procedureHandler = procedures.get(procedureName);
            return Optional.ofNullable(procedureHandler);
        }
        return Optional.empty();
    }

    public void removeTable(String schemaName, String tableName) {
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler != null) {
            NameMap<TableHandler> stringLogicTableConcurrentHashMap = schemaMap.get(schemaName).logicTables();
            if (stringLogicTableConcurrentHashMap != null) {
                stringLogicTableConcurrentHashMap.remove(tableName);
            }
        }
    }

    public static MetadataManager createMetadataManager(Map<String, LogicSchemaConfig> schemaConfigs,
                                                        PrototypeService prototypeService) {
        try {
            MysqlMetadataManager mysqlMetadataManager = new MysqlMetadataManager(schemaConfigs, prototypeService) {
            };
            mysqlMetadataManager.recomputeERRelation();
            return mysqlMetadataManager;
        } catch (Throwable throwable) {
            throw MycatErrorCode.createMycatException(MycatErrorCode.ERR_FETCH_METADATA, "MetadataManager init fail", throwable);
        }
    }

    @SneakyThrows
    public MetadataManager(PrototypeService prototypeService) {
        this.prototypeService = Objects.requireNonNull(prototypeService);
    }

    public void recomputeERRelation() {
        Stream<ShardingTable> shardingTables = this.schemaMap.values().stream().flatMap(i -> i.logicTables().values().stream()).filter(i -> i.getType() == LogicTableType.SHARDING)
                .map(i -> (ShardingTable) i);
        Map<String, List<ShardingTable>> res = shardingTables.collect(Collectors.groupingBy(i -> i.getShardingFuntion().getErUniqueID()));
        this.erTableGroup.clear();
        this.erTableGroup.putAll(res);

        Map<String, Map<String, Partition>> resTargetMap = recomputeTargetMap();
        this.targetTableGroup.clear();
        this.targetTableGroup.putAll(resTargetMap);
    }

    @NotNull
    private Map<String, Map<String, Partition>> recomputeTargetMap() {
        Map<String, Map<String, Partition>> resTargetMap = new HashMap<>();
        Iterator<TableHandler> iterator = this.schemaMap.values().stream().map(i -> i.logicTables().values()).flatMap(i -> i.stream()).iterator();

        while (iterator.hasNext()) {
            TableHandler tableHandler = iterator.next();
            switch (tableHandler.getType()) {
                case SHARDING:
                    ShardingTable shardingTable = (ShardingTable) tableHandler;
                    if (shardingTable.shardingType() == ShardingTableType.SHARDING_INSTANCE_SINGLE_TABLE) {
                        for (Partition backend : shardingTable.getBackends()) {
                            Map<String, Partition> pairs = resTargetMap.computeIfAbsent(backend.getTargetName(), s -> new HashMap<>());
                            pairs.put(shardingTable.getUniqueName(), backend);
                        }
                    }
                    break;
                case GLOBAL:
                    GlobalTable globalTable = (GlobalTable) tableHandler;
                    for (Partition partition : globalTable.getGlobalDataNode()) {
                        Map<String, Partition> pairs = resTargetMap.computeIfAbsent(partition.getTargetName(), s -> new HashMap<>());
                        pairs.put(globalTable.getUniqueName(), partition);
                    }

                    break;
                case NORMAL:
                    NormalTable normalTable = (NormalTable) tableHandler;
                    Partition partition = normalTable.getDataNode();
                    Map<String, Partition> pairs = resTargetMap.computeIfAbsent(partition.getTargetName(), s -> new HashMap<>());
                    pairs.put(normalTable.getUniqueName(), partition);
                    break;
                case CUSTOM:
                    break;
            }
        }
        return resTargetMap;
    }

    public void addSchema(LogicSchemaConfig value) {
        String targetName = value.getTargetName();
        String schemaName = value.getSchemaName();
        SchemaHandlerImpl schemaHandler = new SchemaHandlerImpl(schemaName, targetName);
        schemaMap.put(schemaName, schemaHandler);
        if (targetName != null) {
            Map<String, NormalTableConfig> normalTables = value.getNormalTables();
            Map<String, NormalTableConfig> adds = prototypeService.getDefaultNormalTable(targetName, schemaName, tableName -> {
                NormalTableConfig normalTableConfig = normalTables.get(tableName);
                boolean needLoadCreateTableSQL = true;
                if (normalTableConfig != null) {
                    if (normalTableConfig.getCreateTableSQL() != null) {
                        needLoadCreateTableSQL = false;
                    }
                }
                return needLoadCreateTableSQL;
            });

            for (Map.Entry<String, NormalTableConfig> add : adds.entrySet()) {
                normalTables.computeIfAbsent(add.getKey(), (n) -> add.getValue());
            }
        }

        for (Map.Entry<String, NormalTableConfig> e : value.getNormalTables().entrySet()) {
            String tableName = e.getKey();
            removeTable(schemaName, tableName);
            NormalTableConfig tableConfigEntry = e.getValue();
            try {
                addNormalTable(schemaName, tableName,
                        tableConfigEntry);
            } catch (Throwable throwable) {
                LOGGER.warn("", throwable);
            }
        }
        for (Map.Entry<String, GlobalTableConfig> e : value.getGlobalTables().entrySet()) {
            String tableName = e.getKey();
            removeTable(schemaName, tableName);
            GlobalTableConfig tableConfigEntry = e.getValue();
            List<Partition> backendTableInfos = tableConfigEntry.getBroadcast().stream().map(i -> new BackendTableInfo(i.getTargetName(), schemaName, tableName)).collect(Collectors.toList());
            addGlobalTable(schemaName, tableName,
                    tableConfigEntry,
                    backendTableInfos
            );
        }
        for (Map.Entry<String, ShardingTableConfig> primaryTable : value.getShardingTables().entrySet()) {
            String tableName = primaryTable.getKey();
            ShardingTableConfig tableConfigEntry = primaryTable.getValue();
            removeTable(schemaName, tableName);

            Set<Map.Entry<String, ShardingTableConfig>> indexTables = Optional.ofNullable(tableConfigEntry.getShardingIndexTables())
                    .orElse(Collections.emptyMap()).entrySet();

            List<ShardingIndexTable> shardingIndexTables = new ArrayList<>();
            for (Map.Entry<String, ShardingTableConfig> secondTable : indexTables) {
                String indexTableName = secondTable.getKey();
                String indexName = indexTableName.replace(tableName + "_", "");
                ShardingTableConfig indexTableValue = secondTable.getValue();
                ShardingIndexTable shardingIndexTable = createShardingIndexTable(schemaName, indexName, indexTableName,
                        indexTableValue,
                        getBackendTableInfos(indexTableValue.getPartition()));
                shardingIndexTables.add(shardingIndexTable);
            }

            addShardingTable(schemaName, tableName,
                    tableConfigEntry,
                    getBackendTableInfos(tableConfigEntry.getPartition()),
                    shardingIndexTables);
        }

        for (Map.Entry<String, CustomTableConfig> e : value.getCustomTables().entrySet()) {
            String tableName = e.getKey();
            removeTable(schemaName, tableName);
            CustomTableConfig tableConfigEntry = e.getValue();
            addCustomTable(schemaName,
                    tableName,
                    tableConfigEntry
            );
        }
        for (Map.Entry<String, NormalProcedureConfig> e : value.getNormalProcedures().entrySet()) {
            String procedureName = e.getKey();
            removeProcedure(schemaName, procedureName);
            NormalProcedureConfig normalProcedureConfig = e.getValue();
            addProcedure(schemaName, procedureName, normalProcedureConfig);
        }
    }


    private void addCustomTable(String schemaName,
                                String tableName,
                                CustomTableConfig tableConfigEntry) {
        String createTableSQL = tableConfigEntry.getCreateTableSQL();
        String clazz = tableConfigEntry.getClazz();
        List<SimpleColumnInfo> columns = prototypeService.getColumnInfo(createTableSQL);
        Map<String, IndexInfo> indexInfos = getIndexInfo(createTableSQL, schemaName, columns);
        LogicTable logicTable = new LogicTable(LogicTableType.CUSTOM,
                schemaName, tableName, columns, indexInfos, createTableSQL);
        CustomTableHandlerWrapper customTableHandler = new CustomTableHandlerWrapper(logicTable, clazz, tableConfigEntry.getKvOptions(),
                tableConfigEntry.getListOptions());
        addLogicTable(customTableHandler);
    }

    public boolean addNormalTable(String schemaName,
                                  String tableName,
                                  NormalTableConfig tableConfigEntry) {
        //////////////////////////////////////////////
        NormalBackEndTableInfoConfig dataNode = tableConfigEntry.getLocality();
        List<Partition> partitions = ImmutableList.of(new BackendTableInfo(dataNode.getTargetName(),
                Optional.ofNullable(dataNode.getSchemaName()).orElse(schemaName),
                Optional.ofNullable(dataNode.getTableName()).orElse(tableName)));
        String createTableSQL = Optional.ofNullable(tableConfigEntry.getCreateTableSQL())
                .orElseGet(() -> prototypeService.getCreateTableSQLByJDBC(schemaName, tableName, partitions).orElse(null));
        if (createTableSQL != null) {
            List<SimpleColumnInfo> columns = prototypeService.getColumnInfo(createTableSQL);
            Map<String, IndexInfo> indexInfos = getIndexInfo(createTableSQL, schemaName, columns);
            addLogicTable(LogicTable.createNormalTable(schemaName, tableName, partitions.get(0), columns, indexInfos, createTableSQL, tableConfigEntry));
            return true;
        }
        return false;
    }

    public void addGlobalTable(String schemaName,
                               String orignalTableName,
                               GlobalTableConfig tableConfigEntry,
                               List<Partition> backendTableInfos) {
        //////////////////////////////////////////////
        final String tableName = orignalTableName;
        String createTableSQL = Optional.ofNullable(tableConfigEntry.getCreateTableSQL())
                .orElseGet(() -> prototypeService.getCreateTableSQLByJDBC(schemaName, orignalTableName, backendTableInfos).orElse(null));
        List<SimpleColumnInfo> columns = prototypeService.getColumnInfo(createTableSQL);
        Map<String, IndexInfo> indexInfos = getIndexInfo(createTableSQL, schemaName, columns);

        //////////////////////////////////////////////

        addLogicTable(LogicTable.createGlobalTable(schemaName, tableName, backendTableInfos, columns, indexInfos, createTableSQL, tableConfigEntry));
    }


    public static List<Partition> getBackendTableInfos(ShardingBackEndTableInfoConfig stringListEntry) {
        if (stringListEntry == null) {
            return Collections.emptyList();
        }
        List<List> data = stringListEntry.getData();
        ImmutableList.Builder<BackendTableInfo> builder = ImmutableList.builder();
        if (data != null) {
            int indexCounter = 0;
            for (List datum : data) {
                if (datum.size() == 6) {
                    String target = Objects.toString(datum.get(0));
                    String schema = Objects.toString(datum.get(1));
                    String table = Objects.toString(datum.get(2));
                    int dbIndex = Integer.parseInt(Objects.toString(datum.get(3)));
                    int tableIndex = Integer.parseInt(Objects.toString(datum.get(4)));
                    int index = Integer.parseInt(Objects.toString(datum.get(5)));

                    BackendTableInfo indexBackendTableInfo = new IndexDataNode(target, schema, table, dbIndex, tableIndex, index);
                    builder.add(indexBackendTableInfo);
                } else if (datum.size() == 3) {
                    String target = Objects.toString(datum.get(0));
                    String schema = Objects.toString(datum.get(1));
                    String table = Objects.toString(datum.get(2));

                    builder.add(new IndexDataNode(target, schema, table, 0, indexCounter, indexCounter));
                } else {
                    throw new UnsupportedOperationException("format must be " +
                            "target,schema,table" + " or " +
                            "target,schema,table,dbIndex,tableIndex,index");
                }
                indexCounter++;
            }
        } else if (stringListEntry.getSchemaNames() != null) {
            String schemaNames = stringListEntry.getSchemaNames();
            String tableNames = stringListEntry.getTableNames();
            String targetNames = stringListEntry.getTargetNames();

            String[] targets = SplitUtil.split(targetNames, ',', '$', '-');
            String[] schemas = SplitUtil.split(schemaNames, ',', '$', '-');
            String[] tables = SplitUtil.split(tableNames, ',', '$', '-');


            for (String target : targets) {
                for (String schema : schemas) {
                    for (String table : tables) {
                        SchemaInfo schemaInfo = new SchemaInfo(schema, table);
                        builder.add(new BackendTableInfo(target, schemaInfo));
                    }
                }
            }
        }
        return (List) builder.build();
    }


    @SneakyThrows
    public void addShardingTable(String schemaName,
                                 String orignalTableName,
                                 ShardingTableConfig tableConfigEntry,
                                 List<Partition> backends,
                                 List<ShardingIndexTable> ShardingIndexTables) {
        ShardingTable shardingTable = createShardingTable(schemaName, orignalTableName, tableConfigEntry, backends, ShardingIndexTables);
        addLogicTable(shardingTable);
        for (ShardingTable indexTable : shardingTable.getIndexTables()) {
            addLogicTable(indexTable);
        }
    }

    @SneakyThrows
    public ShardingIndexTable createShardingIndexTable(String schemaName, String indexName,
                                                       String indexTableName,
                                                       ShardingTableConfig secondTableConfig,
                                                       List<Partition> backends) {
        ShardingTable shardingTable = createShardingTable(schemaName, indexTableName, secondTableConfig, backends, Collections.emptyList());
        return new ShardingIndexTable(indexName, shardingTable.getLogicTable(), shardingTable.getBackends(), shardingTable.getShardingFuntion(), null);
    }

    @NotNull
    public ShardingTable createShardingTable(String schemaName,
                                             String orignalTableName,
                                             ShardingTableConfig tableConfigEntry,
                                             List<Partition> backends,
                                             List<ShardingIndexTable> shardingIndexTables) throws Exception {
        ShardingFunction function = tableConfigEntry.getFunction();
        //////////////////////////////////////////////
        String createTableSQL = Optional.ofNullable(tableConfigEntry.getCreateTableSQL()).orElseGet(() -> prototypeService.getCreateTableSQLByJDBC(schemaName, orignalTableName, backends).orElse(null));
        List<SimpleColumnInfo> columns = prototypeService.getColumnInfo(createTableSQL);
        Map<String, IndexInfo> indexInfos = getIndexInfo(createTableSQL, schemaName, columns);
        //////////////////////////////////////////////
        ShardingTable shardingTable = LogicTable.createShardingTable(schemaName, orignalTableName,
                backends, columns, null, indexInfos, createTableSQL, shardingIndexTables, tableConfigEntry);
        shardingTable.setShardingFuntion(PartitionRuleFunctionManager.getRuleAlgorithm(shardingTable, tableConfigEntry.getFunction()));
        for (SimpleColumnInfo column : columns) {
            column.setShardingKey(shardingTable.function().isShardingKey(column.getColumnName()));
        }
        return shardingTable;
    }

    private synchronized void addLogicTable(TableHandler logicTable) {
        String schemaName = logicTable.getSchemaName();
        String tableName = logicTable.getTableName();
        String createTableSQL = logicTable.getCreateTableSQL();
        NameMap<TableHandler> tableMap = schemaMap.get(schemaName).logicTables();
        tableMap.put(tableName, logicTable);
    }


    //////////////////////////////////////////calculate///////////////////////////////

    public boolean containsSchema(String name) {
        return schemaMap.containsKey(Objects.requireNonNull(name), false);
    }

    public int getDefaultStoreNodeNum() {
        ReplicaSelectorManager selectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        long c = selectorRuntime.getReplicaMap()
                .keySet()
                .stream()
                .distinct()
                .filter(i -> i.startsWith("c"))
                .count();
        return (int) c;
    }

    @Getter
    @EqualsAndHashCode
    @ToString
    public static class SimpleRoute {
        String schemaName;
        String tableName;
        String targetName;

        public SimpleRoute(String schemaName, String tableName, String targetName) {
            this.schemaName = Objects.requireNonNull(schemaName);
            this.tableName = Objects.requireNonNull(tableName);
            this.targetName = Objects.requireNonNull(targetName);
        }
    }

    public Optional<Distribution> checkVaildNormalRoute(Set<Pair<String, String>> tableNames) {
        NameMap<SchemaHandler> schemaMap1 = getSchemaMap();
        Distribution leftDistribution = null;
        TableHandler tableHandler = null;
        for (Pair<String, String> tableName : tableNames) {
            SchemaHandler schemaHandler = schemaMap1.get(SQLUtils.normalize(tableName.getKey()), false);
            if (schemaHandler != null) {
                NameMap<TableHandler> logicTables = schemaHandler.logicTables();
                if (logicTables != null) {
                    tableHandler = logicTables.get(SQLUtils.normalize(tableName.getValue()), false);
                    if (tableHandler != null) {
                        if (tableHandler.getType() == LogicTableType.NORMAL || tableHandler.getType() == LogicTableType.GLOBAL) {
                            Distribution rightDistribution;
                            if (tableHandler.getType() == LogicTableType.NORMAL) {
                                NormalTable tableHandler1 = (NormalTable) tableHandler;
                                rightDistribution = Distribution.of(tableHandler1);
                            } else if (tableHandler.getType() == LogicTableType.GLOBAL) {
                                GlobalTable tableHandler1 = (GlobalTable) tableHandler;
                                rightDistribution = Distribution.of(tableHandler1);
                            } else {
                                throw new IllegalArgumentException("unsupported table type:" + tableHandler.getType());
                            }
                            if (leftDistribution == null) {
                                leftDistribution = rightDistribution;
                            } else {
                                Optional<Distribution> newDistribution = leftDistribution.join(rightDistribution);
                                if (newDistribution.isPresent()) {
                                    leftDistribution = newDistribution.get();
                                } else {
                                    return Optional.empty();
                                }
                            }
                            continue;
                        } else {//sharding table
                            return Optional.empty();
                        }
                    }
                }
            }
        }
        if (leftDistribution == null) {
            return Optional.empty();
        }
        return Optional.of(leftDistribution);
    }

    public TableHandler getTable(String schemaName, String tableName) {
        return Optional.ofNullable(schemaMap).map(i -> i.get(schemaName)).map(i -> i.logicTables().get(tableName)).orElse(null);
    }

    public NameMap<SchemaHandler> getSchemaMap() {
        return (NameMap) schemaMap;
    }

    public List<String> showDatabases() {
        return schemaMap.keySet().stream().map(i -> SQLUtils.normalize(i))
                .distinct()
                .filter(i -> !"mycat".equals(i))
                .sorted(Comparator.comparing(s -> s)).collect(Collectors.toList());
    }

    public MetadataManager clear() {
        this.schemaMap.clear();
        return this;
    }


    public static Map<String, IndexInfo> getIndexInfo(String sql, String schemaName, List<SimpleColumnInfo> columnInfoList) {
        return Collections.emptyMap();
    }

    public void createPhysicalTables() {
        this.schemaMap.values().stream().flatMap(i -> i.logicTables().values().stream())
                .filter(i -> i.getType() == LogicTableType.SHARDING || i.getType() == LogicTableType.GLOBAL)
                .parallel()
                .forEach(tableHandler -> tableHandler.createPhysicalTables());
    }

    public List<LogicSchemaConfig> getConfigAsList() {
        return new ArrayList<>(getConfigAsMap().values());
    }

    public Map<String, LogicSchemaConfig> getConfigAsMap() {
        MetadataManager metadataManager = this;
        Map<String, LogicSchemaConfig> schemaConfigs = new HashMap<>();
        for (Map.Entry<String, SchemaHandler> e : metadataManager.getSchemaMap().entrySet()) {
            SchemaHandler schemaHandler = e.getValue();
            LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
            logicSchemaConfig.setSchemaName(e.getKey());
            logicSchemaConfig.setTargetName(schemaHandler.defaultTargetName());
            schemaConfigs.put(e.getKey(), logicSchemaConfig);


            for (TableHandler tableHandler : schemaHandler.logicTables().values()) {
                switch (tableHandler.getType()) {
                    case SHARDING: {
                        ShardingTable shardingTable = (ShardingTable) tableHandler;
                        ShardingTableConfig tableConfig = shardingTable.getTableConfig();
                        if (tableConfig == null) continue;
                        logicSchemaConfig.getShardingTables().put(shardingTable.getTableName(), tableConfig);
                        break;
                    }
                    case GLOBAL: {
                        GlobalTable globalTable = (GlobalTable) tableHandler;
                        logicSchemaConfig.getGlobalTables().put(globalTable.getTableName(), globalTable.getTableConfig());
                        break;
                    }
                    case NORMAL: {
                        NormalTable normalTable = (NormalTable) tableHandler;
                        logicSchemaConfig.getNormalTables().put(normalTable.getTableName(), normalTable.getTableConfig());
                        break;

                    }
                    case CUSTOM:
                        break;
                }
            }

            for (ProcedureHandler procedureHandler : schemaHandler.procedures().values()) {
                switch (procedureHandler.getType()) {
                    case NORMAL:
                        NormalProcedureHandler normalProcedureHandler = (NormalProcedureHandler) procedureHandler;
                        logicSchemaConfig.getNormalProcedures().put(normalProcedureHandler.getName(), normalProcedureHandler.getConfig());
                        break;
                }
            }
        }
        return schemaConfigs;
    }

    public SQLStatement typeInferenceUpdate(SQLStatement statement, String defaultSchema) {
        try {
            if (statement instanceof SQLInsertStatement) {
                SQLInsertStatement sqlInsertStatement = (SQLInsertStatement) statement;
                SQLExprTableSource tableSource = sqlInsertStatement.getTableSource();
                String schemaName = SQLUtils.normalize(Optional.ofNullable(tableSource.getSchema()).orElse(defaultSchema));
                String tableName = SQLUtils.normalize(tableSource.getTableName());
                TableHandler table = this.getTable(schemaName, tableName);
                List<SQLExpr> columns = sqlInsertStatement.getColumns();
                List<SQLInsertStatement.ValuesClause> valuesList = sqlInsertStatement.getValuesList();

                for (SQLInsertStatement.ValuesClause valuesClause : valuesList) {

                    List<SQLExpr> valuesClauseValues = new ArrayList<>(valuesClause.getValues());

                    for (int i = 0; i < columns.size(); i++) {
                        SQLExpr expr = valuesClauseValues.get(i);
                        if (expr instanceof SQLHexExpr) {
                            SQLHexExpr sqlHexExpr = (SQLHexExpr) expr;
                            SQLExpr sqlExpr = columns.get(i);
                            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;
                            String name = sqlIdentifierExpr.normalizedName();
                            SimpleColumnInfo columnByName = table.getColumnByName(name);
                            if (columnByName.getType() != SimpleColumnInfo.Type.BLOB) {
                                valuesClause.replace(expr, sqlHexExpr.toCharExpr());
                            } else {
                                continue;
                            }
                        }
                    }
                }
                return sqlInsertStatement;
            }
        } catch (Exception e) {
            LOGGER.warn("", e);
        }
        return statement;
    }

    public static String getPrototype() {
        return prototype;
    }
}
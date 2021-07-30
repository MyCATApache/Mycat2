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

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.repository.SchemaObject;
import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mysql.MySQLType;
import io.mycat.calcite.table.*;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.querycondition.*;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.router.ShardingTableHandler;
import io.mycat.router.function.IndexDataNode;
import io.mycat.router.mycat1xfunction.PartitionRuleFunctionManager;
import io.mycat.util.*;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Junwen Chen
 **/
public class MetadataManager implements MysqlVariableService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);
    final NameMap<SchemaHandler> schemaMap = new NameMap<>();
    final LoadBalanceManager loadBalanceManager;
    final SequenceGenerator sequenceGenerator;
    final ReplicaSelectorManager replicaSelectorRuntime;
    final JdbcConnectionManager jdbcConnectionManager;

    @Getter
    final String prototype;

    //    public final SchemaRepository TABLE_REPOSITORY = new SchemaRepository(DbType.mysql);
    private final NameMap<Object> globalVariables;
    private final NameMap<Object> sessionVariables;

    @Getter
    private final Map<String, List<ShardingTable>> erTableGroup;
    @Getter
    private final List<GlobalTable> globalTables;
    @Getter
    private final List<NormalTable> normalTables;


    public void removeSchema(String schemaName) {
        schemaMap.remove(schemaName);
    }

    public void addSchema(String schemaName, String dataNode) {
        SchemaHandlerImpl schemaHandler = new SchemaHandlerImpl(schemaName, dataNode);
        schemaMap.put(schemaName, schemaHandler);
    }

    public void addTable(String schemaName,
                         String tableName,
                         ShardingTableConfig tableConfig,
                         ShardingBackEndTableInfoConfig backends,
                         String prototypeServer,
                         List<ShardingIndexTable> indexTables) {
        addShardingTable(schemaName, tableName, tableConfig, prototypeServer, getBackendTableInfos(backends), indexTables);
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

    public static MetadataManager createMetadataManager(List<LogicSchemaConfig> schemaConfigs,
                                                        LoadBalanceManager loadBalanceManager,
                                                        SequenceGenerator sequenceGenerator,
                                                        ReplicaSelectorManager replicaSelectorRuntime,
                                                        JdbcConnectionManager jdbcConnectionManager,
                                                        String prototype) {
        try {
            return new MetadataManager(schemaConfigs,
                    loadBalanceManager,
                    sequenceGenerator,
                    replicaSelectorRuntime,
                    jdbcConnectionManager,
                    prototype);
        } catch (Throwable throwable) {
            throw MycatErrorCode.createMycatException(MycatErrorCode.ERR_FETCH_METADATA, "MetadataManager init fail", throwable);
        }
    }

    @SneakyThrows
    public MetadataManager(List<LogicSchemaConfig> schemaConfigs,
                           LoadBalanceManager loadBalanceManager,
                           SequenceGenerator sequenceGenerator,
                           ReplicaSelectorManager replicaSelectorRuntime,
                           JdbcConnectionManager jdbcConnectionManager,
                           String prototype
    ) {
        this.loadBalanceManager = Objects.requireNonNull(loadBalanceManager);
        this.sequenceGenerator = Objects.requireNonNull(sequenceGenerator);
        this.replicaSelectorRuntime = Objects.requireNonNull(replicaSelectorRuntime);
        this.jdbcConnectionManager = Objects.requireNonNull(jdbcConnectionManager);
        this.prototype = Objects.requireNonNull(prototype);

        Set<String> databases = new HashSet<>();

//        try (DefaultConnection connection = jdbcConnectionManager.getConnection(this.prototype)) {
//            try(RowBaseIterator dbIterator = connection.executeQuery("show databases")){
//                while (dbIterator.next()) {
//                    databases.add(dbIterator.getString(1));
//                }
//            }
//        }

        databases.add("information_schema");
        databases.add("mysql");
        databases.add("performance_schema");


        this.globalVariables = new NameMap<Object>();
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(this.prototype)) {
            try (RowBaseIterator rowBaseIterator = connection.executeQuery(" SHOW GLOBAL VARIABLES;")) {
                while (rowBaseIterator.next()) {
                    globalVariables.put(
                            rowBaseIterator.getString(0),
                            rowBaseIterator.getObject(1)
                    );
                }
            }
        }
        this.sessionVariables = new NameMap<>();
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(this.prototype)) {
            try (RowBaseIterator rowBaseIterator = connection.executeQuery(" SHOW SESSION VARIABLES;")) {
                while (rowBaseIterator.next()) {
                    sessionVariables.put(
                            rowBaseIterator.getString(0),
                            rowBaseIterator.getObject(1)
                    );
                }
            }
        }
        /////////////////////////////////////////////////////////////////
        addInnerTable(schemaConfigs, prototype);


        ///////////////////////////////////////////////////////////////
        //更新新配置里面的信息
        Map<String, LogicSchemaConfig> schemaConfigMap = schemaConfigs
                .stream()
                .collect(Collectors.toMap(k -> k.getSchemaName(), v -> v));

        for (String database : databases) {
            schemaConfigMap.computeIfAbsent(database, s -> {
                LogicSchemaConfig schemaConfig = new LogicSchemaConfig();
                schemaConfig.setSchemaName(database);
                schemaConfig.setTargetName(prototype);
                return schemaConfig;
            });
        }

        for (Map.Entry<String, LogicSchemaConfig> entry : schemaConfigMap.entrySet()) {
            String orignalSchemaName = entry.getKey();
            LogicSchemaConfig value = entry.getValue();
            String targetName = value.getTargetName();
            final String schemaName = orignalSchemaName;
            addSchema(schemaName, targetName);
            if (targetName != null) {
                Map<String, NormalTableConfig> normalTables = value.getNormalTables();
                Map<String, NormalTableConfig> adds = getDefaultNormalTable(targetName, schemaName, tableName -> {
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
                            tableConfigEntry,
                            prototype
                    );
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
                        prototype,
                        backendTableInfos
                );
            }
            for (Map.Entry<String, ShardingTableConfig> primaryTable : value.getShadingTables().entrySet()) {
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
                            prototype,
                            getBackendTableInfos(indexTableValue.getPartition()));
                    shardingIndexTables.add(shardingIndexTable);
                }

                addShardingTable(schemaName, tableName,
                        tableConfigEntry,
                        prototype,
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
        }

        Stream<ShardingTable> shardingTables = this.schemaMap.values().stream().flatMap(i -> i.logicTables().values().stream()).filter(i -> i.getType() == LogicTableType.SHARDING)
                .map(i -> (ShardingTable) i);
        this.erTableGroup = shardingTables.collect(Collectors.groupingBy(i -> i.getShardingFuntion().getErUniqueID()));

        this.globalTables = this.schemaMap.values().stream().flatMap(i -> i.logicTables().values().stream()).filter(i -> i.getType() == LogicTableType.GLOBAL)
                .map(i -> (GlobalTable) i).collect(Collectors.toList());

        this.normalTables = this.schemaMap.values().stream().flatMap(i -> i.logicTables().values().stream()).filter(i -> i.getType() == LogicTableType.NORMAL)
                .map(i -> (NormalTable) i).collect(Collectors.toList());
    }

    private void addInnerTable(List<LogicSchemaConfig> schemaConfigs, String prototype) {
        String schemaName = "mysql";
        String targetName = "prototype";
        String tableName = "proc";

        LogicSchemaConfig logicSchemaConfig = schemaConfigs.stream()
                .filter(i -> schemaName.equals(i.getSchemaName()))
                .findFirst()
                .orElseGet(() -> {
                    LogicSchemaConfig config = new LogicSchemaConfig();
                    config.setSchemaName(schemaName);
                    config.setTargetName(prototype);
                    schemaConfigs.add(config);
                    return config;
                });


        Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
        normalTables.putIfAbsent(tableName, NormalTableConfig.create(schemaName, tableName,
                "CREATE TABLE `mysql`.`proc` (\n" +
                        "  `db` varchar(64) DEFAULT NULL,\n" +
                        "  `name` varchar(64) DEFAULT NULL,\n" +
                        "  `type` enum('FUNCTION','PROCEDURE','PACKAGE', 'PACKAGE BODY'),\n" +
                        "  `specific_name` varchar(64) DEFAULT NULL,\n" +
                        "  `language` enum('SQL'),\n" +
                        "  `sql_data_access` enum('CONTAINS_SQL', 'NO_SQL', 'READS_SQL_DATA', 'MODIFIES_SQL_DATA'),\n" +
                        "  `is_deterministic` enum('YES','NO'),\n" +
                        "  `security_type` enum('INVOKER','DEFINER'),\n" +
                        "  `param_list` blob,\n" +
                        "  `returns` longblob,\n" +
                        "  `body` longblob,\n" +
                        "  `definer` varchar(141),\n" +
                        "  `created` timestamp,\n" +
                        "  `modified` timestamp,\n" +
                        "  `sql_mode` \tset('REAL_AS_FLOAT', 'PIPES_AS_CONCAT', 'ANSI_QUOTES', 'IGNORE_SPACE', 'IGNORE_BAD_TABLE_OPTIONS', 'ONLY_FULL_GROUP_BY', 'NO_UNSIGNED_SUBTRACTION', 'NO_DIR_IN_CREATE', 'POSTGRESQL', 'ORACLE', 'MSSQL', 'DB2', 'MAXDB', 'NO_KEY_OPTIONS', 'NO_TABLE_OPTIONS', 'NO_FIELD_OPTIONS', 'MYSQL323', 'MYSQL40', 'ANSI', 'NO_AUTO_VALUE_ON_ZERO', 'NO_BACKSLASH_ESCAPES', 'STRICT_TRANS_TABLES', 'STRICT_ALL_TABLES', 'NO_ZERO_IN_DATE', 'NO_ZERO_DATE', 'INVALID_DATES', 'ERROR_FOR_DIVISION_BY_ZERO', 'TRADITIONAL', 'NO_AUTO_CREATE_USER', 'HIGH_NOT_PRECEDENCE', 'NO_ENGINE_SUBSTITUTION', 'PAD_CHAR_TO_FULL_LENGTH', 'EMPTY_STRING_IS_NULL', 'SIMULTANEOUS_ASSIGNMENT'),\n" +
                        "  `comment` text,\n" +
                        "  `character_set_client` char(32),\n" +
                        "  `collation_connection` \tchar(32),\n" +
                        "  `db_collation` \tchar(32),\n" +
                        "  `body_utf8` \tlongblob,\n" +
                        "  `aggregate` \tenum('NONE', 'GROUP')\n" +
                        ") ", targetName));

        LogicSchemaConfig mycat = schemaConfigs.stream().filter(i ->
                "mycat".equalsIgnoreCase(i.getSchemaName()))
                .findFirst().orElseGet(() -> {
                    LogicSchemaConfig schemaConfig = new LogicSchemaConfig();
                    schemaConfig.setSchemaName("mycat");
                    schemaConfigs.add(schemaConfig);
                    return schemaConfig;
                });
        Map<String, CustomTableConfig> customTables = mycat.getCustomTables();

        customTables.computeIfAbsent("dual", (n) -> {
            CustomTableConfig tableConfig = CustomTableConfig.builder().build();
            tableConfig.setClazz(DualCustomTableHandler.class.getCanonicalName());
            tableConfig.setCreateTableSQL("create table mycat.dual(id int)");
            return tableConfig;
        });
    }


    private Map<String, NormalTableConfig> getDefaultNormalTable(String targetName, String schemaName, Predicate<String> tableFilter) {
        Set<String> tables = new HashSet<>();
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
            RowBaseIterator tableIterator = connection.executeQuery("show tables from " + schemaName);
            while (tableIterator.next()) {
                tables.add(tableIterator.getString(0));
            }
        }
        Map<String, NormalTableConfig> res = new ConcurrentHashMap<>();
        tables.stream().filter(tableFilter).parallel().forEach(tableName -> {
            NormalBackEndTableInfoConfig normalBackEndTableInfoConfig = new NormalBackEndTableInfoConfig(targetName, schemaName, tableName);
            try {
                res.put(tableName, (new NormalTableConfig(
                        getCreateTableSQLByJDBC(schemaName, tableName,
                                Collections.singletonList(new BackendTableInfo(targetName, schemaName, tableName))),
                        normalBackEndTableInfoConfig)));
            } catch (Throwable e) {
                LOGGER.warn("", e);
            }
        });
        return res;
    }

    private void addCustomTable(String schemaName,
                                String tableName,
                                CustomTableConfig tableConfigEntry) {
        String createTableSQL = tableConfigEntry.getCreateTableSQL();
        String clazz = tableConfigEntry.getClazz();
        List<SimpleColumnInfo> columns = getColumnInfo(createTableSQL);
        Map<String, IndexInfo> indexInfos = getIndexInfo(createTableSQL, schemaName, columns);
        LogicTable logicTable = new LogicTable(LogicTableType.CUSTOM,
                schemaName, tableName, columns, indexInfos, createTableSQL);
        CustomTableHandlerWrapper customTableHandler = new CustomTableHandlerWrapper(logicTable, clazz, tableConfigEntry.getKvOptions(),
                tableConfigEntry.getListOptions());
        addLogicTable(customTableHandler);
    }

    public boolean addNormalTable(String schemaName,
                                  String tableName,
                                  NormalTableConfig tableConfigEntry,
                                  String prototypeServer) {
        //////////////////////////////////////////////
        NormalBackEndTableInfoConfig dataNode = tableConfigEntry.getLocality();
        List<Partition> partitions = ImmutableList.of(new BackendTableInfo(dataNode.getTargetName(),
                Optional.ofNullable(dataNode.getSchemaName()).orElse(schemaName),
                Optional.ofNullable(dataNode.getTableName()).orElse(tableName)));
        String createTableSQL = Optional.ofNullable(tableConfigEntry.getCreateTableSQL())
                .orElseGet(() -> getCreateTableSQLByJDBC(schemaName, tableName, partitions));
        if (createTableSQL != null) {
            List<SimpleColumnInfo> columns = getSimpleColumnInfos(prototypeServer, schemaName, tableName, createTableSQL, partitions);
            Map<String, IndexInfo> indexInfos = getIndexInfo(createTableSQL, schemaName, columns);
            addLogicTable(LogicTable.createNormalTable(schemaName, tableName, partitions.get(0), columns, indexInfos, createTableSQL, tableConfigEntry));
            return true;
        }
        return false;
    }

    public void addGlobalTable(String schemaName,
                               String orignalTableName,
                               GlobalTableConfig tableConfigEntry,
                               String prototypeServer,
                               List<Partition> backendTableInfos) {
        //////////////////////////////////////////////
        final String tableName = orignalTableName;
        String createTableSQL = Optional.ofNullable(tableConfigEntry.getCreateTableSQL())
                .orElseGet(() -> getCreateTableSQLByJDBC(schemaName, orignalTableName, backendTableInfos));
        List<SimpleColumnInfo> columns = getSimpleColumnInfos(prototypeServer, schemaName, tableName, createTableSQL, backendTableInfos);
        Map<String, IndexInfo> indexInfos = getIndexInfo(createTableSQL, schemaName, columns);

        //////////////////////////////////////////////

        LoadBalanceStrategy loadBalance = loadBalanceManager.getLoadBalanceByBalanceName(tableConfigEntry.getBalance());

        addLogicTable(LogicTable.createGlobalTable(schemaName, tableName, backendTableInfos, loadBalance, columns, indexInfos, createTableSQL, tableConfigEntry));
    }


    public static List<Partition> getBackendTableInfos(ShardingBackEndTableInfoConfig stringListEntry) {
        if (stringListEntry == null) {
            return Collections.emptyList();
        }
        List<List> data = stringListEntry.getData();
        ImmutableList.Builder<BackendTableInfo> builder = ImmutableList.builder();
        if (data != null) {
            for (List datum : data) {
                if (datum.size() == 6){
                    String target = Objects.toString(datum.get(0));
                    String schema = Objects.toString(datum.get(1));
                    String table = Objects.toString(datum.get(2));
                    int dbIndex = Integer.parseInt(Objects.toString(datum.get(3)));
                    int tableIndex = Integer.parseInt( Objects.toString(datum.get(4)));
                    int index = Integer.parseInt( Objects.toString(datum.get(5)));

                    BackendTableInfo indexBackendTableInfo = new IndexDataNode(target, schema, table, dbIndex, tableIndex, index);
                    builder.add(indexBackendTableInfo);
                }else if (datum.size() == 3){
                    String target = Objects.toString(datum.get(0));
                    String schema = Objects.toString(datum.get(1));
                    String table = Objects.toString(datum.get(2));

                    builder.add( new BackendTableInfo(target,schema,table));
                }else{
                    throw new UnsupportedOperationException("format must be " +
                            "target,schema,table" +" or "+
                            "target,schema,table,dbIndex,tableIndex,index");
                }
            }
        } else {
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

    private synchronized void accrptDDL(String schemaName, String sql) {
//        TABLE_REPOSITORY.setDefaultSchema(schemaName);
//        TABLE_REPOSITORY.acceptDDL(sql);
    }

    @SneakyThrows
    public void addShardingTable(String schemaName,
                                 String orignalTableName,
                                 ShardingTableConfig tableConfigEntry,
                                 String prototypeServer,
                                 List<Partition> backends,
                                 List<ShardingIndexTable> ShardingIndexTables) {
        ShardingTable shardingTable = createShardingTable(schemaName, orignalTableName, tableConfigEntry, prototypeServer, backends, ShardingIndexTables);
        addLogicTable(shardingTable);
        for (ShardingTable indexTable : shardingTable.getIndexTables()) {
            addLogicTable(indexTable);
        }
    }

    @SneakyThrows
    public ShardingIndexTable createShardingIndexTable(String schemaName, String indexName,
                                                       String indexTableName,
                                                       ShardingTableConfig secondTableConfig,
                                                       String prototypeServer,
                                                       List<Partition> backends) {
        ShardingTable shardingTable = createShardingTable(schemaName, indexTableName, secondTableConfig, prototypeServer, backends, Collections.emptyList());
        return new ShardingIndexTable(indexName, shardingTable.getLogicTable(), shardingTable.getBackends(), shardingTable.getShardingFuntion(), null);
    }

    @NotNull
    public ShardingTable createShardingTable(String schemaName,
                                             String orignalTableName,
                                             ShardingTableConfig tableConfigEntry,
                                             String prototypeServer,
                                             List<Partition> backends,
                                             List<ShardingIndexTable> shardingIndexTables) throws Exception {
        ShardingFuntion function = tableConfigEntry.getFunction();
        //////////////////////////////////////////////
        String createTableSQL = Optional.ofNullable(tableConfigEntry.getCreateTableSQL()).orElseGet(() -> getCreateTableSQLByJDBC(schemaName, orignalTableName, backends));
        List<SimpleColumnInfo> columns = getSimpleColumnInfos(prototypeServer, schemaName, orignalTableName, createTableSQL, backends);
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
        try {
            accrptDDL(schemaName, createTableSQL);
        } catch (Throwable ignored) {

        }
    }


    private List<SimpleColumnInfo> getSimpleColumnInfos(String prototypeServer,
                                                        String schemaName,
                                                        String tableName,
                                                        String createTableSQL,
                                                        List<Partition> backends) {
        List<SimpleColumnInfo> columns = null;
        /////////////////////////////////////////////////////////////////////////////////////////////////

        /////////////////////////////////////////////////////////////////////////////////////////////////
        if (createTableSQL != null) {
            try {
                columns = getColumnInfo(prototypeServer, createTableSQL);
            } catch (Throwable e) {
                LOGGER.warn("无法根据建表sql:{},获取字段信息", createTableSQL, e);
            }
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////
        if (columns == null && backends != null && !backends.isEmpty()) {
            try {
                columns = getColumnInfoBySelectSQLOnJdbc(backends);
            } catch (Throwable e) {
                LOGGER.error("无法根据建表sql:{},获取字段信息", createTableSQL, e);
            }
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////
        if (columns == null && prototypeServer != null) {
            try {
                columns = getSimpleColumnInfos(schemaName, tableName, prototypeServer);
            } catch (Throwable e) {
                LOGGER.error("无法根据建表sql:{},获取字段信息", createTableSQL, e);
            }
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////
        if (columns == null && backends != null && !backends.isEmpty()) {
            try {
                Partition backendTableInfo = backends.get(0);
                String targetName = backendTableInfo.getTargetName();
                String schema = backendTableInfo.getSchema();
                String table = backendTableInfo.getTable();
                try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
                    DatabaseMetaData metaData = connection.getRawConnection().getMetaData();
                    return CalciteConvertors.convertfromDatabaseMetaData(metaData, schema, schema, table);
                }
            } catch (Throwable e) {
                LOGGER.error("无法根据建表sql:{},获取字段信息", createTableSQL, e);
            }
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////
        if (columns == null) {
            throw new UnsupportedOperationException("没有配置建表sql");
        }
        return columns;
    }

    public List<SimpleColumnInfo> getSimpleColumnInfos(String schemaName, String tableName, String targetName) {
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
            Connection rawConnection = connection.getRawConnection();
            DatabaseMetaData metaData = rawConnection.getMetaData();
            return CalciteConvertors.convertfromDatabaseMetaData(metaData, schemaName, schemaName, tableName);
        } catch (Exception e) {
            LOGGER.warn("不能根据schemaName:{} tableName:{} 获取字段信息 {}", schemaName, tableName, e);
        }
        return null;
    }

    private List<SimpleColumnInfo> getColumnInfoBySelectSQLOnJdbc(List<Partition> backends) {
        if (backends.isEmpty()) {
            return null;
        }
        Partition backendTableInfo = backends.get(0);
        String targetName = backendTableInfo.getTargetName();
        String targetSchemaTable = backendTableInfo.getTargetSchemaTable();
        String name = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, true, null);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(name)) {
            Connection rawConnection = connection.getRawConnection();
            String sql = "select * from " + targetSchemaTable + " where 0 ";
            try (Statement statement = rawConnection.createStatement()) {
                statement.setMaxRows(0);
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    resultSet.next();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    JdbcRowMetaData jdbcRowMetaData = new JdbcRowMetaData(metaData);
                    return CalciteConvertors.getColumnInfo(jdbcRowMetaData);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("无法根据jdbc连接获取建表sql:{} {}", backends, e);
        }
        return null;
    }

    private String getCreateTableSQLByJDBC(String schemaName, String tableName, List<Partition> backends) {
        backends = new ArrayList<>(backends);
        backends.add(new BackendTableInfo(prototype, schemaName, tableName));

        if (backends == null || backends.isEmpty()) {
            return null;
        }
        for (Partition backend : backends) {
            try {
                Partition backendTableInfo = backend;
                String targetName = backendTableInfo.getTargetName();
                String targetSchemaTable = backendTableInfo.getTargetSchemaTable();
                String name = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, true, null);
                try (DefaultConnection connection = jdbcConnectionManager.getConnection(name)) {
                    String sql = "SHOW CREATE TABLE " + targetSchemaTable;
                    try (RowBaseIterator rowBaseIterator = connection.executeQuery(sql)) {
                        while (rowBaseIterator.next()) {
                            String string = rowBaseIterator.getString(1);
                            SQLStatement sqlStatement = null;
                            try {
                                sqlStatement = SQLUtils.parseSingleMysqlStatement(string);
                            } catch (Throwable e) {

                            }
                            if (sqlStatement == null) {
                                try {
                                    string = string.substring(0, string.lastIndexOf(')') + 1);
                                    sqlStatement = SQLUtils.parseSingleMysqlStatement(string);
                                } catch (Throwable e) {

                                }
                            }
                            if (sqlStatement instanceof MySqlCreateTableStatement) {
                                MySqlCreateTableStatement sqlStatement1 = (MySqlCreateTableStatement) sqlStatement;

                                sqlStatement1.setTableName(SQLUtils.normalize(tableName));
                                sqlStatement1.setSchema(SQLUtils.normalize(schemaName));//顺序不能颠倒
                                return sqlStatement1.toString();
                            }
                            if (sqlStatement instanceof SQLCreateViewStatement) {
                                SQLCreateViewStatement sqlStatement1 = (SQLCreateViewStatement) sqlStatement;
                                SQLExprTableSource sqlExprTableSource = sqlStatement1.getTableSource();
                                if (!SQLUtils.nameEquals(sqlExprTableSource.getTableName(), tableName) ||
                                        !SQLUtils.nameEquals(sqlExprTableSource.getSchema(), (schemaName))) {
                                    MycatSQLExprTableSourceUtil.setSqlExprTableSource(schemaName, tableName, sqlExprTableSource);
                                    return sqlStatement1.toString();
                                } else {
                                    return string;
                                }
                            }

                        }
                    }
                    try (RowBaseIterator rowBaseIterator = connection.executeQuery("select * from " + targetSchemaTable + " limit 0")) {
                        MycatRowMetaData metaData = rowBaseIterator.getMetaData();
                        MySqlCreateTableStatement mySqlCreateTableStatement = new MySqlCreateTableStatement();
                        mySqlCreateTableStatement.setTableName(tableName);
                        mySqlCreateTableStatement.setSchema(schemaName);
                        int columnCount = metaData.getColumnCount();
                        for (int i = 0; i < columnCount; i++) {
                            int columnType = metaData.getColumnType(i);
                            String type = SQLDataType.Constants.VARCHAR;
                            for (MySQLType value : MySQLType.values()) {
                                if (value.getJdbcType() == columnType) {
                                    type = value.getName();
                                }
                            }
                            mySqlCreateTableStatement.addColumn(metaData.getColumnName(i), type);
                        }
                        return mySqlCreateTableStatement.toString();

                    }
                }
            } catch (Throwable e) {
                LOGGER.error("can not get create table sql from:" + backend.getTargetName() + backend.getTargetSchemaTable(), e);
                continue;
            }
        }


        return null;
    }


    public void resolveMetadata(SQLStatement sqlStatement) {
//        TABLE_REPOSITORY.resolve(sqlStatement, ResolveAllColumn, ResolveIdentifierAlias, CheckColumnAmbiguous);
    }

    //////////////////////////////////////////calculate///////////////////////////////

    public boolean containsSchema(String name) {
        return schemaMap.containsKey(Objects.requireNonNull(name), false);
    }

    @Override
    public Object getGlobalVariable(String name) {
        return globalVariables.get(name.startsWith("@@") ? name.substring(2) : name, false);
    }

    @Override
    public Object getSessionVariable(String name) {
        Object o = sessionVariables.get(name, false);
        return o;
    }

    @Override
    public int getDefaultStoreNodeNum() {
        long c = replicaSelectorRuntime.getReplicaMap()
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

    public boolean checkVaildNormalRoute(Set<Pair<String, String>> tableNames, NameMap<SimpleRoute> tables) {
        NameMap<SchemaHandler> schemaMap1 = getSchemaMap();
        Set<String> targets = new HashSet<>();
        TableHandler tableHandler = null;
        for (Pair<String, String> tableName : tableNames) {
            SchemaHandler schemaHandler = schemaMap1.get(SQLUtils.normalize(tableName.getKey()), false);
            if (schemaHandler != null) {
                NameMap<TableHandler> logicTables = schemaHandler.logicTables();
                if (logicTables != null) {
                    tableHandler = logicTables.get(SQLUtils.normalize(tableName.getValue()), false);
                    if (tableHandler != null) {
                        if (tableHandler.getType() == LogicTableType.NORMAL
                                ||
                                tableHandler.getType() == LogicTableType.GLOBAL) {
                            Partition partition = null;
                            if (tableHandler.getType() == LogicTableType.NORMAL) {
                                NormalTable tableHandler1 = (NormalTable) tableHandler;
                                partition = tableHandler1.getDataNode();
                            } else if (tableHandler.getType() == LogicTableType.GLOBAL) {
                                GlobalTable tableHandler1 = (GlobalTable) tableHandler;
                                int size = tableHandler1.getGlobalDataNode().size();
                                if (size == 0) {
                                    throw new IllegalArgumentException("datanodes of global table is empty");
                                }
                                int i = ThreadLocalRandom.current().nextInt(0, size);
                                partition = tableHandler1.getGlobalDataNode().get(i);
                            } else {
                                throw new IllegalArgumentException("unsupported table type:" + tableHandler.getType());
                            }
                            tables.put(tableHandler.getTableName(),
                                    new SimpleRoute(partition.getSchema(), partition.getTable(), partition.getTargetName()));
                            if (targets.add(partition.getTargetName())) {
                                if (targets.size() > 1) {
                                    return false;
                                }
                            }
                            continue;
                        } else {
                            return false;
                        }
                    }
                }
            }
        }
//        if (tables.values().isEmpty() && tableNames.size() == 1) {
//            Pair<String, String> next = tableNames.iterator().next();
//            tables.put(next.getValue(), new SimpleRoute(next.getKey(), next.getValue(), prototype));
//            targets.add(prototype);
//        }
        return targets.size() == 1;
    }


    public static class Rrs {
        Collection<Partition> backEndTableInfos;
        SQLExprTableSource table;

        public Rrs(Collection<Partition> backEndTableInfos, SQLExprTableSource table) {
            this.backEndTableInfos = backEndTableInfos;
            this.table = table;
        }

        public Collection<Partition> getBackEndTableInfos() {
            return backEndTableInfos;
        }

        public SQLExprTableSource getTable() {
            return table;
        }
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

    public List<SimpleColumnInfo> getColumnInfo(String sql) {
        return getColumnInfo(null, sql);
    }

    public static List<SimpleColumnInfo> getColumnInfoByMysql(String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        if (sqlStatement instanceof MySqlCreateTableStatement) {
            MycatRowMetaData mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData((MySqlCreateTableStatement) sqlStatement);
            return CalciteConvertors.getColumnInfo(Objects.requireNonNull(mycatRowMetaData));
        }
        return null;
    }

    public List<SimpleColumnInfo> getColumnInfo(String prototypeServer, String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        MycatRowMetaData mycatRowMetaData = null;
        if (sqlStatement instanceof MySqlCreateTableStatement) {
            mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData((MySqlCreateTableStatement) sqlStatement);
        }
        if (sqlStatement instanceof SQLCreateViewStatement) {
            SQLCreateViewStatement createViewStatement = (SQLCreateViewStatement) sqlStatement;
            mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData(jdbcConnectionManager, prototypeServer, (SQLCreateViewStatement) sqlStatement);
        }
        return CalciteConvertors.getColumnInfo(Objects.requireNonNull(mycatRowMetaData));
    }

    public static Map<String, IndexInfo> getIndexInfo(String sql, String schemaName, List<SimpleColumnInfo> columnInfoList) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        if (!(sqlStatement instanceof MySqlCreateTableStatement)) {
            return null;
        }
        String tableName = ((MySqlCreateTableStatement) sqlStatement).getTableName();
        List<MySqlTableIndex> mysqlIndexes = ((MySqlCreateTableStatement) sqlStatement).getMysqlIndexes();
        Map<String, IndexInfo> indexInfoMap = new LinkedHashMap<>();
        for (MySqlTableIndex astIndex : mysqlIndexes) {
            // 索引名
            String indexName = SQLUtils.normalize(astIndex.getName().getSimpleName());
            try {
                // 索引列
                List<SimpleColumnInfo> mycatIndexes = new ArrayList<>(columnInfoList);
                Set<String> astIndexSet = astIndex.getColumns().stream()
                        .map(SQLSelectOrderByItem::getExpr)
                        .map(SQLName.class::cast)
                        .map(SQLName::getSimpleName)
                        .map(SQLUtils::normalize)
                        .collect(Collectors.toSet());
                for (int i = 0; i < mycatIndexes.size(); i++) {
                    if (!astIndexSet.contains(mycatIndexes.get(i).getColumnName())) {
                        mycatIndexes.set(i, null);
                    }
                }

                // 覆盖列
                List<SimpleColumnInfo> mycatCoverings = new ArrayList<>(columnInfoList);
                Set<String> astCoveringSet = astIndex.getCovering().stream()
                        .map(SQLName::getSimpleName)
                        .map(SQLUtils::normalize)
                        .collect(Collectors.toSet());
                for (int i = 0; i < mycatCoverings.size(); i++) {
                    if (!astCoveringSet.contains(mycatCoverings.get(i).getColumnName())) {
                        mycatCoverings.set(i, null);
                    }
                }

                // DB分区
                SQLMethodInvokeExpr dbPartitionBy = (SQLMethodInvokeExpr) astIndex.getDbPartitionBy();
                String dbPartitionByMethodName = null;
                List<SimpleColumnInfo> dbPartitionByColumms = new ArrayList<>();
                if (dbPartitionBy != null) {
                    dbPartitionByMethodName = dbPartitionBy.getMethodName();
                    List<SQLExpr> arguments = dbPartitionBy.getArguments();
                    for (SQLExpr argument : arguments) {
                        SQLName sqlName = (SQLName) argument;
                        SimpleColumnInfo columnInfo = columnInfoList.stream()
                                .filter(e -> SQLUtils.nameEquals(e.getColumnName(), sqlName.getSimpleName()))
                                .findFirst()
                                .orElseThrow(() -> new IllegalStateException("解析 " + dbPartitionBy + "时, 发现字段[" + sqlName + "]在列中不存在"));
                        dbPartitionByColumms.add(columnInfo);
                    }
                }

                IndexInfo old = indexInfoMap.put(indexName, new IndexInfo(schemaName, tableName, indexName,
                        mycatIndexes.toArray(new SimpleColumnInfo[0]),
                        mycatCoverings.toArray(new SimpleColumnInfo[0]),
                        new IndexInfo.DBPartitionBy(dbPartitionByMethodName,
                                dbPartitionByColumms.toArray(new SimpleColumnInfo[0]))));
                if (old != null) {
                    throw new IllegalStateException("存在重复的索引名称 " + indexName);
                }
            } catch (ClassCastException e) {
                throw new IllegalStateException("暂时不支持该索引语法" + astIndex);
            }
        }
        return indexInfoMap;
    }

    public void createPhysicalTables() {
        this.schemaMap.values().stream().flatMap(i -> i.logicTables().values().stream())
                .filter(i->i.getType()  == LogicTableType.SHARDING || i.getType()  == LogicTableType.GLOBAL)
                .parallel()
                .forEach(tableHandler -> tableHandler.createPhysicalTables());
    }


}
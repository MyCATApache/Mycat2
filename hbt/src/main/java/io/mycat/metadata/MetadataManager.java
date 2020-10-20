/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.metadata;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.*;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import com.google.common.collect.ImmutableList;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CalciteConvertors;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.hbt3.CustomTable;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.querycondition.*;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.router.ShardingTableHandler;
import io.mycat.router.function.PartitionRuleFunctionManager;
import lombok.Getter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.*;

/**
 * @author Junwen Chen
 **/
public class MetadataManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);
    final ConcurrentHashMap<String, SchemaHandler> schemaMap = new ConcurrentHashMap<>();
    final LoadBalanceManager loadBalanceManager;
    final SequenceGenerator sequenceGenerator;
    final ReplicaSelectorRuntime replicaSelectorRuntime;
    final JdbcConnectionManager jdbcConnectionManager;

    @Getter
    final String prototype;

    public final SchemaRepository TABLE_REPOSITORY = new SchemaRepository(DbType.mysql);
    private final ShardingQueryRootConfig shardingQueryRootConfig;

    public void removeSchema(String schemaName) {
        schemaMap.remove(schemaName);
    }

    public void addSchema(String schemaName, String dataNode) {
        SchemaHandlerImpl schemaHandler = new SchemaHandlerImpl(schemaName, dataNode);
        schemaMap.put(schemaName, schemaHandler);
        schemaMap.put("`" + schemaName + "`", schemaHandler);
    }

    public void addTable(String schemaName, String tableName, ShardingTableConfig tableConfig, List<ShardingBackEndTableInfoConfig> backends, String prototypeServer) {
        addShardingTable(schemaName, tableName, tableConfig, prototypeServer, getBackendTableInfos(backends));
    }

    public void removeTable(String schemaName, String tableName) {
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler != null) {
            Map<String, TableHandler> stringLogicTableConcurrentHashMap = schemaMap.get(schemaName).logicTables();
            if (stringLogicTableConcurrentHashMap != null) {
                stringLogicTableConcurrentHashMap.remove(tableName);
            }
        }
    }


    @SneakyThrows
    public MetadataManager(ShardingQueryRootConfig shardingQueryRootConfig,
                           LoadBalanceManager loadBalanceManager,
                           SequenceGenerator sequenceGenerator,
                           ReplicaSelectorRuntime replicaSelectorRuntime,
                           JdbcConnectionManager jdbcConnectionManager
    ) {
        this.shardingQueryRootConfig = shardingQueryRootConfig;
        this.loadBalanceManager = loadBalanceManager;
        this.sequenceGenerator = sequenceGenerator;
        this.replicaSelectorRuntime = replicaSelectorRuntime;
        this.jdbcConnectionManager = jdbcConnectionManager;
        this.prototype = shardingQueryRootConfig.getPrototype();

        if (this.prototype != null) {
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(this.prototype)) {
                RowBaseIterator dbIterator = connection.executeQuery("show databases");
                Set<String> databases = new HashSet<>();
                while (dbIterator.next()) {
                    databases.add(dbIterator.getString(1));
                }
                for (String schemaName : databases) {
                    addSchema(schemaName, this.prototype);
                }
                for (String schemaName : databases) {
                    loadDefaultNormalTable(connection, schemaName);
                }
            }
        }
        //更新新配置里面的信息
        Map<String, LogicSchemaConfig> schemaConfigMap = shardingQueryRootConfig.getSchemas()
                .stream()
                .collect(Collectors.toMap(k -> k.getSchemaName(), v -> v));
        for (Map.Entry<String, LogicSchemaConfig> entry : schemaConfigMap.entrySet()) {
            String orignalSchemaName = entry.getKey();
            LogicSchemaConfig value = entry.getValue();
            String targetName = value.getTargetName();
            final String schemaName = orignalSchemaName;
            addSchema(schemaName, targetName);
            if (targetName != null) {
                try (DefaultConnection connection = jdbcConnectionManager.getConnection(this.prototype)) {
                    loadDefaultNormalTable(connection, schemaName);
                }
            }
            for (Map.Entry<String, ShardingTableConfig> e : value.getShadingTables().entrySet()) {
                String tableName = e.getKey();
                ShardingTableConfig tableConfigEntry = e.getValue();
                addShardingTable(schemaName, tableName,
                        tableConfigEntry,
                        shardingQueryRootConfig.getPrototype(),
                        getBackendTableInfos(tableConfigEntry.getDataNodes()));
            }

            for (Map.Entry<String, GlobalTableConfig> e : value.getGlobalTables().entrySet()) {
                String tableName = e.getKey();
                GlobalTableConfig tableConfigEntry = e.getValue();
                List<DataNode> backendTableInfos = tableConfigEntry.getDataNodes().stream().map(i -> new BackendTableInfo(i.getTargetName(), schemaName, tableName)).collect(Collectors.toList());
                addGlobalTable(schemaName, tableName,
                        tableConfigEntry,
                        prototype,
                        backendTableInfos
                );
            }
            for (Map.Entry<String, NormalTableConfig> e : value.getNormalTables().entrySet()) {
                String tableName = e.getKey();
                NormalTableConfig tableConfigEntry = e.getValue();
                addNormalTable(schemaName, tableName,
                        tableConfigEntry,
                        prototype
                );
            }
            for (Map.Entry<String, CustomTableConfig> e : value.getCustomTables().entrySet()) {
                String tableName = e.getKey();
                CustomTableConfig tableConfigEntry = e.getValue();
                addCustomTable(schemaName, tableName,
                        tableConfigEntry,
                        shardingQueryRootConfig.getPrototype()
                );
            }
        }
    }

    private void loadDefaultNormalTable(DefaultConnection connection, String schemaName) {
        Set<String> tables = new HashSet<>();
        RowBaseIterator tableIterator = connection.executeQuery("show tables from " + schemaName);
        while (tableIterator.next()) {
            tables.add(tableIterator.getString(1));
        }
        for (String tableName : tables) {
            NormalBackEndTableInfoConfig normalBackEndTableInfoConfig = new NormalBackEndTableInfoConfig(this.prototype, schemaName, tableName);
            try {
                addNormalTable(schemaName, tableName, new NormalTableConfig(null, normalBackEndTableInfoConfig), this.prototype);
            }catch (Throwable e){
                LOGGER.warn("",e);
            }
        }
    }

    private void addCustomTable(String schemaName,
                                String tableName,
                                CustomTableConfig tableConfigEntry,
                                String prototypeServer) {
        String createTableSQL = tableConfigEntry.getCreateTableSQL();
        String clazz = tableConfigEntry.getClazz();
        try {
            Class<?> aClass = Class.forName(clazz);
            Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(
                    String.class,
                    String.class,
                    String.class,
                    Map.class,
                    List.class);
            CustomTable o = (CustomTable) declaredConstructor.newInstance(
                    schemaName,
                    tableName,
                    createTableSQL,
                    tableConfigEntry.getKvOptions(),
                    tableConfigEntry.getListOptions());
            addLogicTable(LogicTable.createCustomTable(o));

        } catch (ClassNotFoundException | NoSuchMethodException|IllegalAccessException| InstantiationException|InvocationTargetException e) {
            LOGGER.error("", e);
        }
    }

    private boolean addNormalTable(String schemaName,
                                String tableName,
                                NormalTableConfig tableConfigEntry,
                                String prototypeServer) {
        //////////////////////////////////////////////
        NormalBackEndTableInfoConfig dataNode = tableConfigEntry.getDataNode();
        List<DataNode> dataNodes = ImmutableList.of(new BackendTableInfo(dataNode.getTargetName(),
                Optional.ofNullable(dataNode.getSchemaName()).orElse(schemaName),
                Optional.ofNullable(dataNode.getTableName()).orElse(tableName)));
        String createTableSQL = Optional.ofNullable(tableConfigEntry.getCreateTableSQL())
                .orElseGet(() -> getCreateTableSQLByJDBC(schemaName, tableName, dataNodes));
        if (createTableSQL!=null){
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(createTableSQL);
            if (sqlStatement instanceof SQLCreateViewStatement){
                SQLCreateViewStatement sqlStatement1 = (SQLCreateViewStatement) sqlStatement;
                MySqlUserName definer = (MySqlUserName)sqlStatement1.getDefiner();
                String userName = SQLUtils.normalize(definer.getUserName()).toLowerCase();
                if (userName.startsWith("mysql.")){
                    return false;
                }
            }
            List<SimpleColumnInfo> columns = getSimpleColumnInfos(prototypeServer, schemaName, tableName, createTableSQL, dataNodes);
            addLogicTable(LogicTable.createNormalTable(schemaName, tableName, dataNodes.get(0), columns, createTableSQL));
            return true;
        }
        return false;
    }

    private void addGlobalTable(String schemaName,
                                String orignalTableName,
                                GlobalTableConfig tableConfigEntry,
                                String prototypeServer,
                                List<DataNode> backendTableInfos) {
        //////////////////////////////////////////////
        final String tableName = orignalTableName;
        String createTableSQL = Optional.ofNullable(tableConfigEntry.getCreateTableSQL())
                .orElseGet(() -> getCreateTableSQLByJDBC(schemaName, orignalTableName, backendTableInfos));
        List<SimpleColumnInfo> columns = getSimpleColumnInfos(prototypeServer, schemaName, tableName, createTableSQL, backendTableInfos);
        //////////////////////////////////////////////

        LoadBalanceStrategy loadBalance = loadBalanceManager.getLoadBalanceByBalanceName(tableConfigEntry.getBalance());

        addLogicTable(LogicTable.createGlobalTable(schemaName, tableName, backendTableInfos, loadBalance, columns, createTableSQL));
    }


    private List<DataNode> getBackendTableInfos(List<ShardingBackEndTableInfoConfig> stringListEntry) {
        if (stringListEntry == null) {
            return Collections.emptyList();
        }
        return stringListEntry.stream().map(t -> {
            SchemaInfo schemaInfo = new SchemaInfo(t.getSchemaName(), t.getTableName());
            return new BackendTableInfo(t.getTargetName(), schemaInfo);
        }).collect(Collectors.toList());
    }

    private synchronized void accrptDDL(String schemaName, String sql) {
        TABLE_REPOSITORY.setDefaultSchema(schemaName);
        TABLE_REPOSITORY.acceptDDL(sql);
    }

    @SneakyThrows
    private  void addShardingTable(String schemaName,
                                  String orignalTableName,
                                  ShardingTableConfig tableConfigEntry,
                                  String prototypeServer,
                                  List<DataNode> backends) {
        //////////////////////////////////////////////
        String createTableSQL = Optional.ofNullable(tableConfigEntry.getCreateTableSQL()).orElseGet(() -> getCreateTableSQLByJDBC(schemaName, orignalTableName, backends));
        List<SimpleColumnInfo> columns = getSimpleColumnInfos(prototypeServer, schemaName, orignalTableName, createTableSQL, backends);
        //////////////////////////////////////////////
        String s = schemaName + "_" + orignalTableName;
        Supplier<String> sequence = sequenceGenerator.getSequence(s);
        ShardingTable shardingTable = LogicTable.createShardingTable(schemaName, orignalTableName, backends, columns, null, sequence, createTableSQL);
        shardingTable.setShardingFuntion(PartitionRuleFunctionManager.INSTANCE.getRuleAlgorithm(shardingTable, tableConfigEntry.getFunction()));
        addLogicTable(shardingTable);
    }

    private synchronized void addLogicTable(TableHandler logicTable) {
        String schemaName = logicTable.getSchemaName();
        String tableName = logicTable.getTableName();
        String createTableSQL = logicTable.getCreateTableSQL();
        Map<String, TableHandler> tableMap;
        tableMap = schemaMap.get(schemaName).logicTables();
        tableMap.put(tableName, logicTable);
        tableMap.put("`" + tableName + "`", logicTable);

        tableMap = schemaMap.get(schemaName).logicTables();
        tableMap.put(tableName, logicTable);
        tableMap.put("`" + tableName + "`", logicTable);
        accrptDDL(schemaName, createTableSQL);
    }


    private List<SimpleColumnInfo> getSimpleColumnInfos(String prototypeServer,
                                                        String schemaName,
                                                        String tableName,
                                                        String createTableSQL,
                                                        List<DataNode> backends) {
        List<SimpleColumnInfo> columns = null;
        /////////////////////////////////////////////////////////////////////////////////////////////////

        /////////////////////////////////////////////////////////////////////////////////////////////////
        if (createTableSQL != null) {
            try {
                columns = getColumnInfo(prototypeServer,createTableSQL);
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
                DataNode backendTableInfo = backends.get(0);
                String targetName = backendTableInfo.getTargetName();
                String schema = backendTableInfo.getSchema();
                String table = backendTableInfo.getTable();
                targetName = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, false, null);
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
        String dataSourceName = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, true, null);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(dataSourceName)) {
            Connection rawConnection = connection.getRawConnection();
            DatabaseMetaData metaData = rawConnection.getMetaData();
            return CalciteConvertors.convertfromDatabaseMetaData(metaData, schemaName, schemaName, tableName);
        } catch (Exception e) {
            LOGGER.warn("不能根据schemaName:{} tableName:{} 获取字段信息 {}", schemaName, tableName, e);
        }
        return null;
    }

    private List<SimpleColumnInfo> getColumnInfoBySelectSQLOnJdbc(List<DataNode> backends) {
        if (backends.isEmpty()) {
            return null;
        }
        DataNode backendTableInfo = backends.get(0);
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

    private String getCreateTableSQLByJDBC(String schemaName, String tableName, List<DataNode> backends) {
        if (backends == null || backends.isEmpty()) {
            return null;
        }
        for (DataNode backend : backends) {
            try {
                DataNode backendTableInfo = backend;
                String targetName = backendTableInfo.getTargetName();
                String targetSchemaTable = backendTableInfo.getTargetSchemaTable();
                String name = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, true, null);
                try (DefaultConnection connection = jdbcConnectionManager.getConnection(name)) {
                    String sql = "SHOW CREATE TABLE " + targetSchemaTable;
                    try (RowBaseIterator rowBaseIterator = connection.executeQuery(sql)) {
                        while (rowBaseIterator.next()) {
                            String string = rowBaseIterator.getString(2);
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
                                    sqlExprTableSource.setSimpleName(SQLUtils.normalize(tableName));
                                    sqlExprTableSource.setSchema(SQLUtils.normalize(schemaName));//顺序不能颠倒
                                    return sqlStatement1.toString();
                                } else {
                                    return string;
                                }
                            }
                        }
                    }
                }
            } catch (Throwable e) {
                LOGGER.error("can not get create table sql from:" + backend.getTargetName() + backend.getTargetSchemaTable(), e);
                continue;
            }
        }


        return null;
    }


    //////////////////////////////////////////////////////function/////////////////////////////////////////////////////

    public Iterable<Map<String, List<String>>> routeInsert(String currentSchema, String sql) {
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql);
        List list = new LinkedList();
        sqlStatementParser.parseStatementList(list);
        return getInsertInfoIterator(currentSchema, (Iterator<MySqlInsertStatement>) list.iterator());
    }

    public Map<String, List<String>> routeInsertFlat(String currentSchema, String sql) {
        Iterable<Map<String, List<String>>> maps = routeInsert(currentSchema, sql);
        HashMap<String, List<String>> res = new HashMap<>();
        for (Map<String, List<String>> map : maps) {
            for (Map.Entry<String, List<String>> e : map.entrySet()) {
                List<String> strings = res.computeIfAbsent(e.getKey(), s -> new ArrayList<>());
                strings.addAll(e.getValue());
            }
        }

        return res;
    }

    public Iterable<Map<String, List<String>>> getInsertInfoIterator(String currentSchemaNameText, Iterator<MySqlInsertStatement> listIterator) {
        final String currentSchemaName = currentSchemaNameText;
        return () -> new Iterator<Map<String, List<String>>>() {
            @Override
            public boolean hasNext() {
                return listIterator.hasNext();
            }

            @Override
            public Map<String, List<String>> next() {
                MySqlInsertStatement statement = listIterator.next();//会修改此对象
                Map<DataNode, List<SQLInsertStatement.ValuesClause>> res = getInsertInfoValuesClause(currentSchemaNameText, statement);
                listIterator.remove();

                //////////////////////////////////////////////////////////////////
                Map<String, List<String>> map = new HashMap<>();
                for (Map.Entry<DataNode, List<SQLInsertStatement.ValuesClause>> entry : res.entrySet()) {
                    DataNode dataNode = entry.getKey();
                    SQLExprTableSource tableSource = statement.getTableSource();
                    tableSource.setExpr(new SQLPropertyExpr(dataNode.getSchema(), dataNode.getTable()));
                    statement.getValuesList().clear();
                    statement.getValuesList().addAll(entry.getValue());
                    List<String> list = map.computeIfAbsent(dataNode.getTargetName(), s12 -> new ArrayList<>());
                    list.add(statement.toString());
                }
                return map;
            }
        };
    }

    //////////////////////////////////////////////////////function/////////////////////////////////////////////////////
    public Map<String, List<String>> getInsertInfoMap(String currentSchemaName, String statement) {
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(statement, DbType.mysql);
        MySqlInsertStatement sqlStatement = (MySqlInsertStatement) sqlStatementParser.parseStatement();
        return getInsertInfoMap(currentSchemaName, sqlStatement);
    }

    public Map<String, List<String>> getInsertInfoIter(String currentSchemaName, String statement) {
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(statement, DbType.mysql);
        MySqlInsertStatement sqlStatement = (MySqlInsertStatement) sqlStatementParser.parseStatement();
        return getInsertInfoMap(currentSchemaName, sqlStatement);
    }

    public Map<String, List<String>> getInsertInfoMap(String currentSchemaName, MySqlInsertStatement statement) {
        Map<String, List<String>> res = new HashMap<>();
        Map<DataNode, List<SQLInsertStatement.ValuesClause>> insertInfo = getInsertInfoValuesClause(currentSchemaName, statement);
        SQLExprTableSource tableSource = statement.getTableSource();
        for (Map.Entry<DataNode, List<SQLInsertStatement.ValuesClause>> backendTableInfoListEntry : insertInfo.entrySet()) {
            statement.getValuesList().clear();
            DataNode key = backendTableInfoListEntry.getKey();
            statement.getValuesList().addAll(backendTableInfoListEntry.getValue());
            tableSource.setExpr(new SQLPropertyExpr(key.getSchema(), key.getTable()));
            List<String> strings = res.computeIfAbsent(key.getTargetName(), s -> new ArrayList<>());
            strings.add(statement.toString());
        }
        return res;
    }

    public Map<DataNode, List<SQLInsertStatement.ValuesClause>> getInsertInfoValuesClause(String currentSchemaName, String statement) {
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(statement, DbType.mysql);
        MySqlInsertStatement sqlStatement = (MySqlInsertStatement) sqlStatementParser.parseStatement();
        return getInsertInfoValuesClause(currentSchemaName, sqlStatement);
    }

    public Map<DataNode, List<SQLInsertStatement.ValuesClause>> getInsertInfoValuesClause(String currentSchemaName, MySqlInsertStatement statement) {
        String s = statement.getTableSource().getSchema();
        String schema = SQLUtils.normalize(s == null ? currentSchemaName : s);
        String tableName = SQLUtils.normalize(statement.getTableSource().getTableName());
        TableHandler logicTable = schemaMap.get(Objects.requireNonNull(schema)).logicTables().get(tableName);
        if (!(logicTable instanceof ShardingTableHandler)) {
            throw new AssertionError();
        }
        List<SQLExpr> columns = statement.getColumns();
        Iterable<SQLInsertStatement.ValuesClause> originValuesList = statement.getValuesList();
        Iterable<SQLInsertStatement.ValuesClause> outValuesList;
        List<SimpleColumnInfo> simpleColumnInfos;
        if (columns == null) {
            simpleColumnInfos = logicTable.getColumns();
        } else {
            simpleColumnInfos = new ArrayList<>(logicTable.getColumns().size());
            for (SQLExpr column : columns) {
                String columnName = SQLUtils.normalize(column.toString());
                try {
                    SimpleColumnInfo columnByName = Objects.requireNonNull(logicTable.getColumnByName(columnName));
                    simpleColumnInfos.add(columnByName);
                } catch (NullPointerException e) {
                    throw new MycatException("未知字段:" + columnName);
                }
            }
        }
        Supplier<String> stringSupplier = logicTable.nextSequence();
        if (logicTable.isAutoIncrement() && stringSupplier != null) {
            if (!simpleColumnInfos.contains(logicTable.getAutoIncrementColumn())) {
                simpleColumnInfos.add(logicTable.getAutoIncrementColumn());
                ///////////////////////////////修改参数//////////////////////////////
                statement.getColumns().add(new SQLIdentifierExpr(logicTable.getAutoIncrementColumn().getColumnName()));
                ///////////////////////////////修改参数//////////////////////////////
                outValuesList = () -> StreamSupport.stream(originValuesList.spliterator(), false)
                        .peek(i -> i.getValues()
                                .add(SQLExprUtils.fromJavaObject(stringSupplier.get())))
                        .iterator();
            } else {
                int index = simpleColumnInfos.indexOf(logicTable.getAutoIncrementColumn());
                outValuesList = () -> StreamSupport.stream(originValuesList.spliterator(), false)
                        .peek(i -> {
                            List<SQLExpr> values = i.getValues();
                            SQLExpr sqlExpr = values.get(index);
                            if (sqlExpr instanceof SQLNullExpr || sqlExpr == null) {
                                values.set(index, SQLExprUtils.fromJavaObject(stringSupplier.get()));
                            }
                        })
                        .iterator();
            }
        } else {
            outValuesList = originValuesList;
        }

        return getBackendTableInfoListMap(simpleColumnInfos, (ShardingTableHandler) logicTable, outValuesList);
    }

    public Map<DataNode, List<SQLInsertStatement.ValuesClause>> getBackendTableInfoListMap(List<SimpleColumnInfo> columns, ShardingTableHandler logicTable, Iterable<SQLInsertStatement.ValuesClause> valuesList) {
        int index;
        HashMap<DataNode, List<SQLInsertStatement.ValuesClause>> res = new HashMap<>(1);
        for (SQLInsertStatement.ValuesClause valuesClause : valuesList) {
            DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
            index = 0;
            for (SQLExpr valueText : valuesClause.getValues()) {
                SimpleColumnInfo simpleColumnInfo = columns.get(index);
                if (valueText instanceof SQLValuableExpr) {
                    String value = SQLUtils.normalize(Objects.toString(((SQLValuableExpr) valueText).getValue()));
                    dataMappingEvaluator.assignment(simpleColumnInfo.getColumnName(), value);
                }  //                    throw new UnsupportedOperationException();

                index++;
            }
            List<DataNode> calculate = logicTable.function().calculate(dataMappingEvaluator.getColumnMap());
            if (calculate.size() != 1) {
                throw new UnsupportedOperationException("插入语句多于1个目标:" + valuesList);
            }
            DataNode endTableInfo = calculate.get(0);
            List<SQLInsertStatement.ValuesClause> valuesGroup = res.computeIfAbsent(endTableInfo, backEndTableInfo -> new ArrayList<>(1));
            valuesGroup.add(valuesClause);
        }
        return res;
    }

    public Map<String, List<String>> rewriteSQL(String currentSchema, String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        resolveMetadata(sqlStatement);
        ConditionCollector conditionCollector = new ConditionCollector();
        sqlStatement.accept(conditionCollector);
        Rrs rrs = assignment(conditionCollector.getRootQueryDataRange(), currentSchema);
        Map<String, List<String>> sqls = new HashMap<>();
        for (DataNode endTableInfo : rrs.getBackEndTableInfos()) {
            SQLExprTableSource table = rrs.getTable();
            table.setExpr(new SQLPropertyExpr(endTableInfo.getSchema(), endTableInfo.getTable()));
            List<String> list = sqls.computeIfAbsent(endTableInfo.getTargetName(), s -> new ArrayList<>());
            list.add(SQLUtils.toMySqlString(sqlStatement));
        }
        return sqls;
    }

    public void resolveMetadata(SQLStatement sqlStatement) {
        TABLE_REPOSITORY.resolve(sqlStatement, ResolveAllColumn, ResolveIdentifierAlias, CheckColumnAmbiguous);
    }

    //////////////////////////////////////////calculate///////////////////////////////
    private Rrs assignment(
            QueryDataRange queryDataRange, String wapperSchemaName) {
        String schemaName = wapperSchemaName;
        String tableName = null;
        SQLExprTableSource table = null;
        if (queryDataRange.getTableSource() != null) {
            table = queryDataRange.getTableSource();
            SchemaObject schemaObject = Objects.requireNonNull(table.getSchemaObject(), "meet unknown table " + table);
            schemaName = SQLUtils.normalize(schemaObject.getSchema().getName());
            tableName = SQLUtils.normalize(schemaObject.getName());
        }
        ShardingTableHandler logicTable = (ShardingTableHandler) schemaMap.get(schemaName).logicTables().get(tableName);
        DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();

        for (ColumnValue equalValue : queryDataRange.getEqualValues()) {
            dataMappingEvaluator.assignment(equalValue.getColumn().computeAlias(), Objects.toString(equalValue.getValue()));

        }
        List<ColumnRangeValue> rangeValues1 = queryDataRange.getRangeValues();
        for (ColumnRangeValue columnRangeValue : rangeValues1) {
            dataMappingEvaluator.assignmentRange(columnRangeValue.getColumn().computeAlias(), Objects.toString(columnRangeValue.getBegin()), Objects.toString(columnRangeValue.getEnd()));
        }
        List<DataNode> calculate = logicTable.function().calculate(dataMappingEvaluator.getColumnMap());
        return new Rrs(calculate, table);
    }


    public static class Rrs {
        Collection<DataNode> backEndTableInfos;
        SQLExprTableSource table;

        public Rrs(Collection<DataNode> backEndTableInfos, SQLExprTableSource table) {
            this.backEndTableInfos = backEndTableInfos;
            this.table = table;
        }

        public Collection<DataNode> getBackEndTableInfos() {
            return backEndTableInfos;
        }

        public SQLExprTableSource getTable() {
            return table;
        }
    }

    public TableHandler getTable(String schemaName, String tableName) {
        return Optional.ofNullable(schemaMap).map(i -> i.get(schemaName)).map(i -> i.logicTables().get(tableName)).orElse(null);
    }

    public Map<String, SchemaHandler> getSchemaMap() {
        return (Map) schemaMap;
    }

    public List<String> showDatabases() {
        return schemaMap.keySet().stream().map(i -> SQLUtils.normalize(i)).distinct().sorted(Comparator.comparing(s -> s)).collect(Collectors.toList());
    }

    public MetadataManager clear() {
        this.schemaMap.clear();
        return this;
    }
    public  List<SimpleColumnInfo> getColumnInfo(String prototypeServer, String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        MycatRowMetaData mycatRowMetaData = null;
        if (sqlStatement instanceof MySqlCreateTableStatement){
            mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData((MySqlCreateTableStatement)sqlStatement);
        }
        if (sqlStatement instanceof SQLCreateViewStatement){
            SQLCreateViewStatement  createViewStatement = (SQLCreateViewStatement)sqlStatement;
            mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData(jdbcConnectionManager,prototypeServer,(SQLCreateViewStatement)sqlStatement);
        }
        return CalciteConvertors.getColumnInfo(Objects.requireNonNull(mycatRowMetaData));
    }

}
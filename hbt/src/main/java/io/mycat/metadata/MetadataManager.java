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
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import io.mycat.BackendTableInfo;
import io.mycat.MycatConfig;
import io.mycat.MycatException;
import io.mycat.SchemaInfo;
import io.mycat.calcite.CalciteConvertors;
import io.mycat.config.GlobalTableConfig;
import io.mycat.config.ShardingQueryRootConfig;
import io.mycat.config.ShardingTableConfig;
import io.mycat.config.SharingFuntionRootConfig;
import io.mycat.plug.PlugRuntime;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.queryCondition.*;
import io.mycat.router.RuleFunction;
import io.mycat.router.function.PartitionRuleFunctionManager;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.*;
import static io.mycat.calcite.CalciteConvertors.getColumnInfo;

/**
 * @author Junwen Chen
 **/
public enum MetadataManager {
    INSTANCE;
    private final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);
    final ConcurrentHashMap<String, SchemaHandler> schemaMap = new ConcurrentHashMap<>();

    private final SchemaRepository TABLE_REPOSITORY = new SchemaRepository(DbType.mysql);

    public void removeSchema(String schemaName) {
        schemaMap.remove(schemaName);
    }

    public void addSchema(String schemaName,String dataNode) {
        SchemaHandlerImpl schemaHandler = new SchemaHandlerImpl(dataNode);
        schemaMap.computeIfAbsent(schemaName, s -> schemaHandler);
        schemaMap.computeIfAbsent("`" + schemaName + "`", s -> schemaHandler);
    }

    public void addTable(String schemaName, String tableName, ShardingTableConfig tableConfig, List<ShardingQueryRootConfig.BackEndTableInfoConfig> backends, ShardingQueryRootConfig.PrototypeServer prototypeServer) {
        addShardingTable(schemaName, tableName, tableConfig, prototypeServer, getBackendTableInfos(backends));
    }

    public void removeTable(String schemaName, String tableName) {
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler!=null) {
            Map<String, TableHandler> stringLogicTableConcurrentHashMap = schemaMap.get(schemaName).logicTables();
            if (stringLogicTableConcurrentHashMap != null) {
                stringLogicTableConcurrentHashMap.remove(tableName);
            }
        }
    }


    public void load(MycatConfig mycatConfig) {
        ShardingQueryRootConfig shardingQueryRootConfig = mycatConfig.getMetadata();
        if (shardingQueryRootConfig!=null) {
            for (Map.Entry<String, ShardingQueryRootConfig.LogicSchemaConfig> entry : shardingQueryRootConfig.getSchemas()
                    .stream()
                    .collect(Collectors.toMap(k->k.getSchemaName(),v->v)).entrySet()) {
                String orignalSchemaName = entry.getKey();
                ShardingQueryRootConfig.LogicSchemaConfig value = entry.getValue();
                String targetName = value.getTargetName();
                final String schemaName = orignalSchemaName.toLowerCase();
                addSchema(schemaName,targetName);
                for (Map.Entry<String, ShardingTableConfig> e : value.getShadingTables().entrySet()) {
                    String tableName = e.getKey().toLowerCase();
                    ShardingTableConfig tableConfigEntry = e.getValue();
                    addShardingTable(schemaName, tableName, tableConfigEntry, shardingQueryRootConfig.getPrototype(), getBackendTableInfos(tableConfigEntry.getDataNodes()));
                }

                for (Map.Entry<String, GlobalTableConfig> e : value.getGlobalTables().entrySet()) {
                    String tableName = e.getKey().toLowerCase();
                    GlobalTableConfig tableConfigEntry = e.getValue();
                    addGlobalTable(schemaName, tableName,
                            tableConfigEntry,
                            shardingQueryRootConfig.getPrototype(),
                            getBackendTableInfos(tableConfigEntry.getDataNodes()),
                            getBackendTableInfos(tableConfigEntry.getDataNodes())
                    );

                }


            }
        }
    }

    private void addGlobalTable(String schemaName,
                                String orignalTableName,
                                GlobalTableConfig tableConfigEntry,
                                ShardingQueryRootConfig.PrototypeServer prototypeServer,
                                List<BackendTableInfo> backendTableInfos,
                                List<BackendTableInfo> readOnly) {
        //////////////////////////////////////////////
        final String tableName = orignalTableName.toLowerCase();
        String createTableSQL = tableConfigEntry.getCreateTableSQL();
        List<SimpleColumnInfo> columns = getSimpleColumnInfos(schemaName, prototypeServer, tableName, createTableSQL);
        //////////////////////////////////////////////

        LoadBalanceStrategy loadBalance = PlugRuntime.INSTCANE.getLoadBalanceByBalanceName(tableConfigEntry.getBalance());

        addLogicTable(LogicTable.createGlobalTable(schemaName, tableName, backendTableInfos, readOnly, loadBalance, columns, createTableSQL));
    }


    @SneakyThrows
    MetadataManager() {

    }

    private List<BackendTableInfo> getBackendTableInfos(List<ShardingQueryRootConfig.BackEndTableInfoConfig> stringListEntry) {
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

    private void addShardingTable(String schemaName, String orignalTableName, ShardingTableConfig tableConfigEntry, ShardingQueryRootConfig.PrototypeServer prototypeServer, List<BackendTableInfo> backends) {
        //////////////////////////////////////////////
        final String tableName = orignalTableName.toLowerCase();
        String createTableSQL = tableConfigEntry.getCreateTableSQL();
        List<SimpleColumnInfo> columns = getSimpleColumnInfos(schemaName, prototypeServer, tableName, createTableSQL);
        //////////////////////////////////////////////
        Supplier<String> sequence = SequenceGenerator.INSTANCE.getSequence(schemaName.toLowerCase() + "_" + orignalTableName.toLowerCase());
        if (sequence == null) {
            sequence = SequenceGenerator.INSTANCE.getSequence(orignalTableName.toUpperCase());
        }
        addLogicTable(LogicTable.createShardingTable(schemaName, tableName, backends, columns, getShardingInfo(columns, tableConfigEntry.getColumns()), createTableSQL, sequence));
    }

    private void addLogicTable(TableHandler logicTable) {
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

    private Map<SimpleColumnInfo.@NonNull ShardingType, SimpleColumnInfo.ShardingInfo> getShardingInfo(List<SimpleColumnInfo> columns, List<ShardingQueryRootConfig.Column> columnMap) {
        return columnMap.stream().map(entry1 -> {
            SharingFuntionRootConfig.ShardingFuntion function = entry1.getFunction();
            RuleFunction ruleAlgorithm = PartitionRuleFunctionManager.INSTANCE.
                    getRuleAlgorithm(entry1.getColumnName(), function.getClazz(), function.getProperties(), function.getRanges());
            SimpleColumnInfo.ShardingType shardingType = SimpleColumnInfo.ShardingType.valueOf(entry1.getShardingType());
            SimpleColumnInfo found = null;
            for (SimpleColumnInfo i : columns) {
                if (entry1.getColumnName().equals(i.getColumnName())) {
                    found = i;
                    break;
                }
            }
            SimpleColumnInfo simpleColumnInfo = Objects.requireNonNull(found);
            return new SimpleColumnInfo.ShardingInfo(simpleColumnInfo, shardingType, entry1.getMap(), ruleAlgorithm);
        }).collect(Collectors.toMap(k -> k.getShardingType(), k -> k));
    }

    private List<SimpleColumnInfo> getSimpleColumnInfos(String schemaName, ShardingQueryRootConfig.PrototypeServer prototypeServer, String tableName, String createTableSQL) {
        List<SimpleColumnInfo> columns = null;
        if (createTableSQL != null) {
            columns = getColumnInfo(createTableSQL);
        } else if (prototypeServer != null) {
            columns = CalciteConvertors.getSimpleColumnInfos(schemaName, tableName, prototypeServer.getUrl(), prototypeServer.getUser(), prototypeServer.getPassword());
        }
        if (columns == null) {
            throw new UnsupportedOperationException();
        }
        return columns;
    }


    //////////////////////////////////////////////////////function/////////////////////////////////////////////////////

    public static Iterable<Map<String, List<String>>> routeInsert(String currentSchema, String sql) {
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql);
        List list = new LinkedList();
        sqlStatementParser.parseStatementList(list);
        return MetadataManager.INSTANCE.getInsertInfoIterator(currentSchema, (Iterator<MySqlInsertStatement>) list.iterator());
    }

    public static Map<String, List<String>> routeInsertFlat(String currentSchema, String sql) {
        Iterable<Map<String, List<String>>> maps = routeInsert(currentSchema, sql);
        Stream<Map<String, List<String>>> stream = StreamSupport.stream(maps.spliterator(), false);
        return stream.flatMap(i -> i.entrySet().stream())
                .collect(Collectors.groupingBy(k -> k.getKey(), Collectors.mapping(i -> i.getValue(), Collectors.reducing(new ArrayList<String>(), (list, list2) -> {
                    list.addAll(list2);
                    return list;
                }))));
    }

    public Iterable<Map<String, List<String>>> getInsertInfoIterator(String currentSchemaNameText, Iterator<MySqlInsertStatement> listIterator) {
        final String currentSchemaName = currentSchemaNameText.toLowerCase();
        return () -> new Iterator<Map<String, List<String>>>() {
            @Override
            public boolean hasNext() {
                return listIterator.hasNext();
            }

            @Override
            public Map<String, List<String>> next() {
                MySqlInsertStatement statement = listIterator.next();//会修改此对象
                Map<BackendTableInfo, List<SQLInsertStatement.ValuesClause>> res = getInsertInfoValuesClause(currentSchemaNameText, statement);
                listIterator.remove();

                //////////////////////////////////////////////////////////////////
                Map<String, List<String>> map = new HashMap<>();
                for (Map.Entry<BackendTableInfo, List<SQLInsertStatement.ValuesClause>> entry : res.entrySet()) {
                    BackendTableInfo key = entry.getKey();
                    SchemaInfo schemaInfo = key.getSchemaInfo();
                    SQLExprTableSource tableSource = statement.getTableSource();
                    tableSource.setExpr(new SQLPropertyExpr(schemaInfo.getTargetSchema(), schemaInfo.getTargetTable()));
                    statement.getValuesList().clear();
                    statement.getValuesList().addAll(entry.getValue());
                    List<String> list = map.computeIfAbsent(key.getTargetName(), s12 -> new ArrayList<>());
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
        Map<BackendTableInfo, List<SQLInsertStatement.ValuesClause>> insertInfo = getInsertInfoValuesClause(currentSchemaName, statement);
        SQLExprTableSource tableSource = statement.getTableSource();
        for (Map.Entry<BackendTableInfo, List<SQLInsertStatement.ValuesClause>> backendTableInfoListEntry : insertInfo.entrySet()) {
            statement.getValuesList().clear();
            BackendTableInfo key = backendTableInfoListEntry.getKey();
            statement.getValuesList().addAll(backendTableInfoListEntry.getValue());
            SchemaInfo schemaInfo = key.getSchemaInfo();
            tableSource.setExpr(new SQLPropertyExpr(schemaInfo.getTargetSchema(), schemaInfo.getTargetTable()));
            List<String> strings = res.computeIfAbsent(key.getTargetName(), s -> new ArrayList<>());
            strings.add(statement.toString());
        }
        return res;
    }

    public Map<BackendTableInfo, List<SQLInsertStatement.ValuesClause>> getInsertInfoValuesClause(String currentSchemaName, String statement) {
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(statement, DbType.mysql);
        MySqlInsertStatement sqlStatement = (MySqlInsertStatement) sqlStatementParser.parseStatement();
        return getInsertInfoValuesClause(currentSchemaName, sqlStatement);
    }

    public Map<BackendTableInfo, List<SQLInsertStatement.ValuesClause>> getInsertInfoValuesClause(String currentSchemaName, MySqlInsertStatement statement) {
        String s = statement.getTableSource().getSchema();
        String schema = s == null ? currentSchemaName : s;
        String tableName = SQLUtils.normalize(statement.getTableSource().getTableName()).toLowerCase();
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
                simpleColumnInfos.add(logicTable.getColumnByName(SQLUtils.normalize(column.toString()).toLowerCase()));
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

    public Map<BackendTableInfo, List<SQLInsertStatement.ValuesClause>> getBackendTableInfoListMap(List<SimpleColumnInfo> columns, ShardingTableHandler logicTable, Iterable<SQLInsertStatement.ValuesClause> valuesList) {
        int index;
        HashMap<BackendTableInfo, List<SQLInsertStatement.ValuesClause>> res = new HashMap<>(1);
        for (SQLInsertStatement.ValuesClause valuesClause : valuesList) {
            DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
            index = 0;
            for (SQLExpr valueText : valuesClause.getValues()) {
                SimpleColumnInfo simpleColumnInfo = columns.get(index);
                if (valueText instanceof SQLValuableExpr) {
                    String value = SQLUtils.normalize(Objects.toString(((SQLValuableExpr) valueText).getValue()));
                    dataMappingEvaluator.assignment(false, simpleColumnInfo.getColumnName(), value);
                } else {
                    throw new UnsupportedOperationException();
                }
                index++;
            }
            List<BackendTableInfo> calculate = dataMappingEvaluator.calculate(logicTable);
            if (calculate.size() != 1) {
                throw new UnsupportedOperationException();
            }
            BackendTableInfo endTableInfo = calculate.get(0);
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
        Rrs rrs = assignment(conditionCollector.isFailureIndeterminacy(), conditionCollector.getRootQueryDataRange(), currentSchema);
        Map<String, List<String>> sqls = new HashMap<>();
        for (BackendTableInfo endTableInfo : rrs.getBackEndTableInfos()) {
            SchemaInfo schemaInfo = endTableInfo.getSchemaInfo();
            SQLExprTableSource table = rrs.getTable();
            table.setExpr(new SQLPropertyExpr(schemaInfo.getTargetSchema(), schemaInfo.getTargetTable()));
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
            boolean fail,
            QueryDataRange queryDataRange, String wapperSchemaName) {
        String schemaName = wapperSchemaName;
        String tableName = null;
        SQLExprTableSource table = null;
        if (queryDataRange.getTableSource() != null) {
            table = queryDataRange.getTableSource();
            SchemaObject schemaObject = Objects.requireNonNull(table.getSchemaObject(), "meet unknown table " + table);
            schemaName = SQLUtils.normalize(schemaObject.getSchema().getName()).toLowerCase();
            tableName = SQLUtils.normalize(schemaObject.getName()).toLowerCase();
        }


        Set<BackendTableInfo> backEndTableInfos1 = new HashSet<>(1);
        if (queryDataRange.getEqualValues() != null && !queryDataRange.getEqualValues().isEmpty()) {
            for (ColumnValue equalValue : queryDataRange.getEqualValues()) {
                SQLTableSource tableSource = equalValue.getTableSource();
                if (tableSource instanceof SQLExprTableSource) {
                    table = (SQLExprTableSource) tableSource;
                    SchemaObject schemaObject = table.getSchemaObject();
                    schemaName = SQLUtils.normalize(schemaObject.getSchema().getName()).toLowerCase();
                    tableName = SQLUtils.normalize(schemaObject.getName()).toLowerCase();

                    if (fail) {
                        break;
                    }
                    TableHandler logicTable = schemaMap.get(schemaName).logicTables().get(tableName);
                    if (logicTable.getType() != LogicTableType.SHARDING) {
                        throw new AssertionError();
                    }
                    DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
                    dataMappingEvaluator.assignment(false, equalValue.getColumn().computeAlias(), Objects.toString(equalValue.getValue()));
                    List<BackendTableInfo> calculate = dataMappingEvaluator.calculate((ShardingTableHandler) logicTable);
                    if (calculate.size() == 1) {
                        backEndTableInfos1.addAll(calculate);
                        break;
                    }
                }
            }
        } else {
            List<ColumnRangeValue> rangeValues = queryDataRange.getRangeValues();
            if (rangeValues != null && !rangeValues.isEmpty()) {
                for (ColumnRangeValue rangeValue : rangeValues) {
                    SQLTableSource tableSource = rangeValue.getTableSource();
                    if (tableSource instanceof SQLExprTableSource) {
                        table = (SQLExprTableSource) tableSource;
                        SchemaObject schemaObject = table.getSchemaObject();
                        schemaName = SQLUtils.normalize(schemaObject.getSchema().getName()).toLowerCase();
                        tableName = SQLUtils.normalize(schemaObject.getName()).toLowerCase();

                        if (fail) {
                            break;
                        }

                        ShardingTableHandler logicTable = (ShardingTableHandler) schemaMap.get(schemaName).logicTables().get(tableName);
                        DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
                        dataMappingEvaluator.assignmentRange(false, SQLUtils.normalize(rangeValue.getColumn().getColumnName()), Objects.toString(rangeValue.getBegin()), Objects.toString(rangeValue.getEnd()));
                        List<BackendTableInfo> backendTableInfos = dataMappingEvaluator.calculate(logicTable);
                        backEndTableInfos1.addAll(backendTableInfos);
                    }
                }
            }
        }
        if (backEndTableInfos1.isEmpty() && schemaName != null) {
            TableHandler logicTable = schemaMap.get(schemaName).logicTables().get(tableName);
            backEndTableInfos1.addAll(((ShardingTableHandler) logicTable).getShardingBackends());
        }
        return new Rrs(backEndTableInfos1, table);
    }

    public List<BackendTableInfo> getMapBackEndTableInfo(String schemaName, String tableName, Map<String, String> map) {
        try {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
            ShardingTableHandler logicTable = (ShardingTableHandler) this.schemaMap.get(schemaName).logicTables().get(tableName);
            DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                dataMappingEvaluator.assignment(false, entry.getKey(), entry.getValue());
            }
            return dataMappingEvaluator.calculate(logicTable);
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new MycatException("{0} {1} {2} can not calculate", schemaName, tableName, Objects.toString(map));
        }
    }

    public BackendTableInfo getNatrueBackEndTableInfo(String schemaName, String tableName, String partitionValue) {
        try {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
            ShardingTableHandler logicTable = (ShardingTableHandler) this.schemaMap.get(schemaName).logicTables().get(tableName);
            return getBackendTableInfo(partitionValue, logicTable);
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new MycatException("{0} {1} {2} can not calculate", schemaName, tableName, partitionValue);
        }
    }

    private BackendTableInfo getBackendTableInfo(String partitionValue, ShardingTableHandler logicTable) {
        DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
        dataMappingEvaluator.assignment(false, logicTable.getNatureTableColumnInfo().getColumnInfo().getColumnName(), partitionValue);
        return dataMappingEvaluator.calculate(logicTable).get(0);
    }

    public List<BackendTableInfo> getNatrueBackEndTableInfo(String schemaName, String tableName, String startValue, String endValue) {
        try {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
            ShardingTableHandler logicTable = (ShardingTableHandler) this.schemaMap.get(schemaName).logicTables().get(tableName);
            DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
            dataMappingEvaluator.assignment(false, startValue, endValue);
            return dataMappingEvaluator.calculate(logicTable);
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new MycatException("{0} {1} {2} {3} can not calculate", schemaName, tableName, startValue, endValue);
        }
    }

    public static class Rrs {
        Set<BackendTableInfo> backEndTableInfos;
        SQLExprTableSource table;

        public Rrs(Set<BackendTableInfo> backEndTableInfos, SQLExprTableSource table) {
            this.backEndTableInfos = backEndTableInfos;
            this.table = table;
        }

        public Set<BackendTableInfo> getBackEndTableInfos() {
            return backEndTableInfos;
        }

        public SQLExprTableSource getTable() {
            return table;
        }
    }

    public Map<String, SchemaHandler> getSchemaMap() {
        return (Map) schemaMap;
    }}
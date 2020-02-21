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
package io.mycat.calcite.metadata;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
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
import io.mycat.config.ShardingQueryRootConfig;
import io.mycat.config.SharingFuntionRootConfig;
import io.mycat.queryCondition.ColumnRangeValue;
import io.mycat.queryCondition.ColumnValue;
import io.mycat.queryCondition.ConditionCollector;
import io.mycat.queryCondition.QueryDataRange;
import io.mycat.router.RuleFunction;
import io.mycat.router.function.PartitionRuleFunctionManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.*;
import static io.mycat.calcite.CalciteConvertors.getColumnInfo;
import static io.mycat.calcite.metadata.SimpleColumnInfo.ShardingType.*;

/**
 * @author Junwen Chen
 **/
public enum MetadataManager {
    INSTANCE;
    private final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);
    final ConcurrentHashMap<String, ConcurrentHashMap<String, LogicTable>> logicTableMap = new ConcurrentHashMap<>();

    private final SchemaRepository TABLE_REPOSITORY = new SchemaRepository(DbType.mysql);


    public void removeSchema(String schemaName) {
        logicTableMap.remove(schemaName);
    }

    public void addSchema(String schemaName) {
        logicTableMap.computeIfAbsent(schemaName, s -> new ConcurrentHashMap<>());
        logicTableMap.computeIfAbsent("`" + schemaName + "`", s -> new ConcurrentHashMap<>());
    }

    public void addTable(String schemaName, String tableName, ShardingQueryRootConfig.LogicTableConfig tableConfig, List<ShardingQueryRootConfig.BackEndTableInfoConfig> backends, ShardingQueryRootConfig.PrototypeServer prototypeServer) {
        addSchema(schemaName);
        addLogicTable(schemaName, tableName, tableConfig, prototypeServer, getBackendTableInfos(backends));
    }

    public void removeTable(String schemaName, String tableName) {
        ConcurrentHashMap<String, LogicTable> stringLogicTableConcurrentHashMap = logicTableMap.get(schemaName);
        if (stringLogicTableConcurrentHashMap != null) {
            stringLogicTableConcurrentHashMap.remove(tableName);
        }
    }

    public List<String> getDatabases() {
        return logicTableMap.keySet().stream().sorted().collect(Collectors.toList());
    }

    public void load(MycatConfig mycatConfig) {
        ShardingQueryRootConfig shardingQueryRootConfig = mycatConfig.getMetadata();
        for (Map.Entry<String, ShardingQueryRootConfig.LogicSchemaConfig> entry : shardingQueryRootConfig.getSchemas().entrySet()) {
            String orignalSchemaName = entry.getKey();
            ShardingQueryRootConfig.LogicSchemaConfig value = entry.getValue();
            final String schemaName = orignalSchemaName.toLowerCase();
            for (Map.Entry<String, ShardingQueryRootConfig.LogicTableConfig> e : value.getTables().entrySet()) {
                String tableName = e.getKey().toLowerCase();
                ShardingQueryRootConfig.LogicTableConfig tableConfigEntry = e.getValue();
                addLogicTable(schemaName, tableName, tableConfigEntry, shardingQueryRootConfig.getPrototype(), getBackendTableInfos(tableConfigEntry.getDataNodes()));
            }
        }
    }


    @Getter
    public static class LogicTable {
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

    private void addLogicTable(String schemaName, String orignalTableName, ShardingQueryRootConfig.LogicTableConfig tableConfigEntry, ShardingQueryRootConfig.PrototypeServer prototypeServer, List<BackendTableInfo> backends) {
        //////////////////////////////////////////////
        final String tableName = orignalTableName.toLowerCase();
        String createTableSQL = tableConfigEntry.getCreateTableSQL();
        List<SimpleColumnInfo> columns = getSimpleColumnInfos(schemaName, prototypeServer, tableName, createTableSQL);
        //////////////////////////////////////////////

        List<ShardingQueryRootConfig.Column> columnMap = tableConfigEntry.getColumns();
        Map<SimpleColumnInfo.@NonNull ShardingType, SimpleColumnInfo.ShardingInfo> shardingInfo = getShardingInfo(columns, columnMap);
        LogicTable logicTable = new LogicTable(schemaName, tableName, backends, columns, shardingInfo, createTableSQL);
        Map<String, LogicTable> tableMap;
        tableMap = logicTableMap.computeIfAbsent(schemaName, s -> new ConcurrentHashMap<>());
        tableMap.put(tableName, logicTable);
        tableMap.put("`" + tableName + "`", logicTable);

        tableMap = logicTableMap.computeIfAbsent("`" + schemaName + "`", s -> new ConcurrentHashMap<>());
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

    //////////////////////////////////////////////////////function/////////////////////////////////////////////////////
    public Iterable<Map<String, List<String>>> getInsertInfoIterator(String currentSchemaNameText, Iterator<MySqlInsertStatement> listIterator) {
        final String currentSchemaName = currentSchemaNameText.toLowerCase();
        return () -> new Iterator<Map<String, List<String>>>() {
            @Override
            public boolean hasNext() {
                return listIterator.hasNext();
            }

            @Override
            public Map<String, List<String>> next() {
                MySqlInsertStatement statement = listIterator.next();
                String s = statement.getTableSource().getSchema();
                String schema = s == null ? currentSchemaName : s;
                String tableName = SQLUtils.normalize(statement.getTableSource().getTableName()).toLowerCase();
                List<SQLExpr> columns = statement.getColumns();
                if (columns == null){
                    columns = (List)statement.getTableSource().getColumns();
                }
                String[] columnList = new String[columns.size()];
                int index = 0;
                for (SQLExpr column : columns) {
                    columnList[index] = SQLUtils.normalize(column.toString()).toLowerCase();
                    index++;
                }
                LogicTable logicTable = logicTableMap.get(schema).get(tableName);
                List<SQLInsertStatement.ValuesClause> valuesList = statement.getValuesList();
                ArrayList<SQLInsertStatement.ValuesClause> valuesClauses = new ArrayList<>(valuesList);
                valuesList.clear();
                HashMap<BackendTableInfo, List<SQLInsertStatement.ValuesClause>> res = new HashMap<>();
                for (SQLInsertStatement.ValuesClause valuesClause : valuesClauses) {
                    DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
                    index = 0;
                    for (SQLExpr valueText : valuesClause.getValues()) {
                        String s1 = columnList[index++];
                        if (valueText instanceof SQLValuableExpr) {
                            String value = SQLUtils.normalize(Objects.toString(((SQLValuableExpr) valueText).getValue()));
                            dataMappingEvaluator.assignment(false, s1, value);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    }
                    List<BackendTableInfo> calculate = dataMappingEvaluator.calculate(logicTable);
                    if (calculate.size() != 1) {
                        throw new UnsupportedOperationException();
                    }
                    BackendTableInfo endTableInfo = calculate.get(0);
                    List<SQLInsertStatement.ValuesClause> valuesGroup = res.computeIfAbsent(endTableInfo, backEndTableInfo -> new ArrayList<>());
                    valuesGroup.add(valuesClause);
                }
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

    public Map<String, List<String>> rewriteSQL(String currentSchema, String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        TABLE_REPOSITORY.resolve(sqlStatement, ResolveAllColumn, ResolveIdentifierAlias, CheckColumnAmbiguous);
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
                    LogicTable logicTable = logicTableMap.get(schemaName).get(tableName);
                    DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
                    dataMappingEvaluator.assignment(false, equalValue.getColumn().computeAlias(), Objects.toString(equalValue.getValue()));
                    List<BackendTableInfo> calculate = dataMappingEvaluator.calculate(logicTable);
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

                        LogicTable logicTable = logicTableMap.get(schemaName).get(tableName);
                        DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
                        dataMappingEvaluator.assignmentRange(false, SQLUtils.normalize(rangeValue.getColumn().getColumnName()), Objects.toString(rangeValue.getBegin()), Objects.toString(rangeValue.getEnd()));
                        List<BackendTableInfo> backendTableInfos = dataMappingEvaluator.calculate(logicTable);
                        backEndTableInfos1.addAll(backendTableInfos);
                    }
                }
            }
        }
        if (backEndTableInfos1.isEmpty() && schemaName != null) {
            LogicTable logicTable = logicTableMap.get(schemaName).get(tableName);
            backEndTableInfos1.addAll(logicTable.backends);
        }
        return new Rrs(backEndTableInfos1, table);
    }

    public List<BackendTableInfo> getMapBackEndTableInfo(String schemaName, String tableName, Map<String, String> map) {
        try {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
            LogicTable logicTable = this.logicTableMap.get(schemaName).get(tableName);
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
            LogicTable logicTable = this.logicTableMap.get(schemaName).get(tableName);
            return getBackendTableInfo(partitionValue, logicTable);
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new MycatException("{0} {1} {2} can not calculate", schemaName, tableName, partitionValue);
        }
    }

    private BackendTableInfo getBackendTableInfo(String partitionValue, LogicTable logicTable) {
        DataMappingEvaluator dataMappingEvaluator = new DataMappingEvaluator();
        dataMappingEvaluator.assignment(false, logicTable.natureTableColumnInfo.getColumnInfo().getColumnName(), partitionValue);
        return dataMappingEvaluator.calculate(logicTable).get(0);
    }

    public List<BackendTableInfo> getNatrueBackEndTableInfo(String schemaName, String tableName, String startValue, String endValue) {
        try {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
            LogicTable logicTable = this.logicTableMap.get(schemaName).get(tableName);
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

    public ConcurrentHashMap<String, ConcurrentHashMap<String, LogicTable>> getLogicTableMap() {
        return logicTableMap;
    }}
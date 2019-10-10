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
package io.mycat.calcite;

import cn.lightfish.sqlEngine.ast.optimizer.queryCondition.ColumnRangeValue;
import cn.lightfish.sqlEngine.ast.optimizer.queryCondition.ColumnValue;
import cn.lightfish.sqlEngine.ast.optimizer.queryCondition.ConditionCollector;
import cn.lightfish.sqlEngine.ast.optimizer.queryCondition.QueryDataRange;
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
import com.alibaba.fastsql.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastsql.sql.repository.Schema;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import io.mycat.ConfigRuntime;
import io.mycat.MycatException;
import io.mycat.config.ConfigFile;
import io.mycat.config.YamlUtil;
import io.mycat.config.shardingQuery.ShardingQueryRootConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.router.RuleAlgorithm;
import io.mycat.router.function.PartitionRuleAlgorithmManager;
import lombok.*;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.*;

/**
 * @author Junwen Chen
 **/
public enum MetadataManager {
    INSATNCE;
    final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);
    final ConcurrentHashMap<String, Map<String, List<BackEndTableInfo>>> schemaBackendMetaMap = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Map<String, List<SimpleColumnInfo>>> schemaColumnMetaMap = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Map<String, DataMappingConfig>> schemaDataMappingMetaMap = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Map<String, JdbcTable>> logicTableMap = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Map<String, String>> logicTableCreateSQLMap = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Map<String, DataMappingEvaluator>> logicTableDataMappingOiginalEvaluator = new ConcurrentHashMap<>();
    final ConcurrentHashMap<JdbcDataSource, Set<SchemaInfo>> physicalTableMap = new ConcurrentHashMap<>();
    //    final ConcurrentHashMap<BackEndTableInfo, Map<String, SchemaInfo>> backEndTableInfoTableMapping = new ConcurrentHashMap<>();
    public final SchemaRepository TABLE_REPOSITORY = new SchemaRepository(DbType.mysql);

    MetadataManager() {
        final String charset = "UTF-8";
        System.setProperty("saffron.default.charset", charset);
        System.setProperty("saffron.default.nationalcharset", charset);
        System.setProperty("saffron.default.collat​​ion.name", charset + "$ en_US");
        PartitionRuleAlgorithmManager.INSTANCE.initFunctions(ConfigRuntime.INSTCANE.load().getConfig(ConfigFile.FUNCTIONS));
        ShardingQueryRootConfig shardingQueryRootConfig = (ShardingQueryRootConfig) ConfigRuntime.INSTCANE.getConfig(ConfigFile.SHARDING_QUERY);
        if (shardingQueryRootConfig == null) {
            addSchema("TESTDB");
            List<BackEndTableInfo> tableInfos = Arrays.asList(
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db1").targetTable("TRAVELRECORD").build()).build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db1").targetTable("TRAVELRECORD2").build()).build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db1").targetTable("TRAVELRECORD3").build()).build(),

                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db2").targetTable("TRAVELRECORD").build()).build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db2").targetTable("TRAVELRECORD2").build()).build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db2").targetTable("TRAVELRECORD3").build()).build(),

                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db3").targetTable("TRAVELRECORD").build()).build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db3").targetTable("TRAVELRECORD2").build()).build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db3").targetTable("TRAVELRECORD3").build()).build()
            );
            addTable("TESTDB", "TRAVELRECORD", tableInfos);
            Map<String, String> properties = new HashMap<>();
            properties.put("partitionCount", "2,1");
            properties.put("partitionLength", "256,512");
            addTableDataMapping("TESTDB", "TRAVELRECORD", Arrays.asList("ID"), "partitionByLong", properties, Collections.emptyMap());
            addCreateTableSQL("TESTDB", "TRAVELRECORD", "CREATE TABLE `travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL,\n" +
                    "  `d` double DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
            addCreateTableSQL("TESTDB", "ADDRESS", "create table `address` (\n" +
                    "\t`id` int (11),\n" +
                    "\t`addressname` varchar (80)\n" +
                    "); \n");

            List<BackEndTableInfo> tableInfos2 = Arrays.asList(
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db1").targetTable("address").build()).build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db2").targetTable("address").build()).build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db3").targetTable("address").build()).build()
            );

            addTable("TESTDB", "ADDRESS", tableInfos2);
            properties.put("partitionCount", "2,1");
            properties.put("partitionLength", "256,512");
            addTableDataMapping("TESTDB", "ADDRESS", Arrays.asList("ID"), "partitionByLong", properties, Collections.emptyMap());

            ShardingQueryRootConfig rootConfig = new ShardingQueryRootConfig();
            List<ShardingQueryRootConfig.LogicSchemaConfig> metaMap = rootConfig.getSchemas();
            schemaBackendMetaMap.forEach((schemaName, tableList) -> {
                List<ShardingQueryRootConfig.LogicTableConfig> list = new ArrayList<>();
                metaMap.add(new ShardingQueryRootConfig.LogicSchemaConfig(schemaName, list));
                for (Map.Entry<String, List<BackEndTableInfo>> entry : tableList.entrySet()) {
                    String tableName = entry.getKey().toLowerCase();
                    List<ShardingQueryRootConfig.BackEndTableInfoConfig> backEndTableInfoConfigList = new ArrayList<>();
                    List<BackEndTableInfo> endTableInfos = entry.getValue();
                    for (BackEndTableInfo b : endTableInfos) {
                        backEndTableInfoConfigList.add(new ShardingQueryRootConfig.BackEndTableInfoConfig(
                                b.getDataNodeName(), b.getReplicaName(), b.getHostName(), b.getSchemaInfo().getTargetSchema(), b.getSchemaInfo().getTargetTable()));
                    }
                    DataMappingConfig dataMappingConfig = this.schemaDataMappingMetaMap.get(schemaName).get(tableName);
                    ShardingQueryRootConfig.LogicTableConfig logicTableConfig = new ShardingQueryRootConfig.LogicTableConfig(tableName, backEndTableInfoConfigList, dataMappingConfig.columnName,
                            dataMappingConfig.ruleAlgorithm.name(), dataMappingConfig.ruleAlgorithm.getProt(), dataMappingConfig.ruleAlgorithm.getRanges(), null);
                    list.add(logicTableConfig);
                }
            });

            String dump = YamlUtil.dump(rootConfig);
            System.out.println(dump);
        } else {
            for (ShardingQueryRootConfig.LogicSchemaConfig entry : shardingQueryRootConfig.getSchemas()) {
                String schemaName = entry.getSchemaName();
                addSchema(schemaName);
                for (ShardingQueryRootConfig.LogicTableConfig tableConfigEntry : entry.getTables()) {
                    String tableName = tableConfigEntry.getTableName();
                    ArrayList<BackEndTableInfo> list = new ArrayList<>();
                    for (ShardingQueryRootConfig.BackEndTableInfoConfig b : tableConfigEntry.getQueryPhysicalTable()) {
                        list.add(new BackEndTableInfo(b.getDataNodeName(), b.getReplicaName(), b.getHostName(), new SchemaInfo(schemaName.toLowerCase(), tableName.toLowerCase(), b.getSchemaName().toLowerCase(), b.getTableName().toLowerCase())));
                    }
                    addTable(schemaName, tableName, list);
                    List<String> columns = tableConfigEntry.getColumns();
                    String function = tableConfigEntry.getFunction();
                    Map<String, String> properties = tableConfigEntry.getProperties();
                    Map<String, String> ranges = tableConfigEntry.getRanges();
                    addTableDataMapping(schemaName, tableName, columns, function, properties, ranges);
                    String createTableSQL = tableConfigEntry.getCreateTableSQL();
                    addCreateTableSQL(schemaName, tableName, createTableSQL);
                }
            }

        }
        for (Map.Entry<String, Map<String, List<BackEndTableInfo>>> entry : this.schemaBackendMetaMap.entrySet()) {
            String schemaName = entry.getKey();

            HashMap<String, List<BackEndTableInfo>> res = new HashMap<>();
            Map<String, List<BackEndTableInfo>> tableList = entry.getValue();
            for (Map.Entry<String, List<BackEndTableInfo>> listEntry : tableList.entrySet()) {
                String tableName = listEntry.getKey();
                for (BackEndTableInfo next : listEntry.getValue()) {
                    JdbcDataSource physical = getPhysical(next);
                    SchemaInfo schemaInfo = next.getSchemaInfo();
                    schemaInfo.targetTable = schemaInfo.targetTable.toLowerCase();
                    schemaInfo.targetSchema = schemaInfo.targetSchema.toLowerCase();
                    schemaInfo.logicTable = schemaInfo.logicTable.toLowerCase();
                    schemaInfo.logicSchema = schemaInfo.logicSchema.toLowerCase();
                    addTable(physicalTableMap, physical, next.getSchemaInfo());

                    res.put(tableName, listEntry.getValue());
                    res.put(tableName.toLowerCase(), listEntry.getValue());
                }
            }
            entry.setValue(res);

        }
        if (schemaColumnMetaMap.isEmpty()) {
            schemaColumnMetaMap.putAll(CalciteConvertors.columnInfoListByDataSourceWithCreateTableSQL(schemaBackendMetaMap, this.logicTableCreateSQLMap));
        }
        logicTableCreateSQLMap.forEach((schemaName, tableMap) -> {
            Schema schema = TABLE_REPOSITORY.findSchema(schemaName);
            tableMap.forEach((table, sql) -> {
                TABLE_REPOSITORY.acceptDDL(sql);
            });
        });
        if (logicTableMap.isEmpty()) {
            buildLogicTable();
        }
//
//        String value = "2000";
//
//        BackEndTableInfo backEndTableInfo = getBackEndTableInfo("TESTDB", "TRAVELRECORD", value);
//        JdbcDataSource datasource = backEndTableInfo.getDatasource(true, null);
//        Set<SchemaInfo> phySchemaInfos = physicalTableMap.get(datasource);

        String sql = "select * from TESTDB.travelrecord where id between 1 and 999";
        Map<BackEndTableInfo, String> sqls = rewriteUpdateSQL("testdb", sql);

        Collection<String> values = sqls.values();

        String insertSQL = "insert into `travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`, `blob`, `d`) values('1','1','2019-09-12','222','9','ssss','666.666'),('999',NULL,NULL,NULL,NULL,NULL,NULL);";
        String currentSchemaText = "TESTDB";
        String currentSchemaName = currentSchemaText.toLowerCase();
        MySqlStatementParser sqlStatementParser = new MySqlStatementParser(insertSQL);
        List<SQLStatement> statementList = new LinkedList<>();
        sqlStatementParser.parseStatementList(statementList, -1, null);
        Iterator<MySqlInsertStatement> listIterator = (Iterator) statementList.listIterator();
        Iterator<Map<BackEndTableInfo, String>> iterator = getInsertInfoIterator(currentSchemaName, listIterator);

        Map<BackEndTableInfo, String> next = null;
        while (iterator.hasNext()) {
            next = iterator.next();
            System.out.println(next);
        }

    }

    private Iterator<Map<BackEndTableInfo, String>> getInsertInfoIterator(String currentSchemaName, Iterator<MySqlInsertStatement> listIterator) {
        return new Iterator<Map<BackEndTableInfo, String>>() {
            @Override
            public boolean hasNext() {
                return listIterator.hasNext();
            }

            @Override
            public Map<BackEndTableInfo, String> next() {
                MySqlInsertStatement statement = listIterator.next();
                String s = statement.getTableSource().getSchema();
                String schema = s == null ? currentSchemaName : s;
                String tableName = SQLUtils.normalize(statement.getTableSource().getTableName()).toLowerCase();
                List<SQLExpr> columns = statement.getColumns();
                String[] columnList = new String[columns.size()];
                int index = 0;
                for (SQLExpr column : statement.getColumns()) {
                    columnList[index] = SQLUtils.normalize(column.toString()).toLowerCase();
                    index++;
                }
                DataMappingEvaluator dataMappingEvaluator = logicTableDataMappingOiginalEvaluator.get(schema).get(tableName).copy();
                List<SQLInsertStatement.ValuesClause> valuesList = statement.getValuesList();
                ArrayList<SQLInsertStatement.ValuesClause> valuesClauses = new ArrayList<>(valuesList);
                valuesList.clear();
                HashMap<BackEndTableInfo, List<SQLInsertStatement.ValuesClause>> res = new HashMap<>();
                for (SQLInsertStatement.ValuesClause valuesClause : valuesClauses) {
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
                    int[] calculate = dataMappingEvaluator.calculate();
                    int backendIndex = calculate[0];
                    List<BackEndTableInfo> backEndTableInfos = schemaBackendMetaMap.get(schema).get(tableName);
                    BackEndTableInfo endTableInfo = backEndTableInfos.get(backendIndex);
                    List<SQLInsertStatement.ValuesClause> valuesGroup = res.computeIfAbsent(endTableInfo, backEndTableInfo -> new ArrayList<>());
                    valuesGroup.add(valuesClause);
                }
                listIterator.remove();

                //////////////////////////////////////////////////////////////////
                HashMap<BackEndTableInfo, String> map = new HashMap<>();
                for (
                        Map.Entry<BackEndTableInfo, List<SQLInsertStatement.ValuesClause>> entry : res.entrySet()) {
                    BackEndTableInfo key = entry.getKey();
                    SchemaInfo schemaInfo = key.getSchemaInfo();
                    SQLExprTableSource tableSource = statement.getTableSource();
                    tableSource.setExpr(new SQLPropertyExpr(schemaInfo.getTargetSchema(), schemaInfo.getTargetTable()));
                    statement.getValuesList().clear();
                    statement.getValuesList().addAll(entry.getValue());
                    map.put(key, statement.toString());
                }

                return map;
            }
        }

                ;
    }

    public Map<BackEndTableInfo, String> rewriteUpdateSQL(String currentSchema, String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        TABLE_REPOSITORY.resolve(sqlStatement, ResolveAllColumn, ResolveIdentifierAlias, CheckColumnAmbiguous);
        ConditionCollector conditionCollector = new ConditionCollector();
        sqlStatement.accept(conditionCollector);
        Rrs rrs = assignment(conditionCollector.isFailureIndeterminacy(), conditionCollector.getRootQueryDataRange());
        String schemaName = rrs.getSchemaName();
        String tableName = rrs.getTableName();
        Map<String, SchemaInfo> replacerMap = new HashMap<>();
        Map<BackEndTableInfo, String> sqls = new HashMap<>();
        for (BackEndTableInfo endTableInfo : rrs.getBackEndTableInfos()) {
            SchemaInfo schemaInfo = endTableInfo.getSchemaInfo();
            replacerMap.put(schemaName.toLowerCase() + "." + tableName.toLowerCase(), schemaInfo);
            SQLStatement clone = sqlStatement.clone();
            clone.accept(new MysqlTableReplacer(replacerMap, currentSchema.toLowerCase()));
            sqls.put(endTableInfo, SQLUtils.toMySqlString(clone));
        }
        return sqls;
    }

    private Rrs assignment(boolean fail, QueryDataRange queryDataRange) {
        List<ColumnValue> equalValues = queryDataRange.getEqualValues();
        String schemaName = null;
        String tableName = null;
        List<BackEndTableInfo> backEndTableInfos1 = null;
        if (equalValues != null && !equalValues.isEmpty()) {
            for (ColumnValue equalValue : queryDataRange.getEqualValues()) {
                SQLTableSource tableSource = equalValue.getTableSource();
                if (tableSource instanceof SQLExprTableSource) {
                    SQLExprTableSource table = (SQLExprTableSource) tableSource;
                    SchemaObject schemaObject = table.getSchemaObject();
                    schemaName = SQLUtils.normalize(schemaObject.getSchema().getName()).toLowerCase();
                    tableName = SQLUtils.normalize(schemaObject.getName()).toLowerCase();

                    if (fail) {
                        break;
                    }

                    DataMappingEvaluator dataMappingEvaluator = logicTableDataMappingOiginalEvaluator
                            .get(schemaName)
                            .get(tableName)
                            .copy();
                    dataMappingEvaluator.assignment(false, equalValue.getColumn().computeAlias(), Objects.toString(equalValue.getValue()));
                    int[] calculate = dataMappingEvaluator.calculate();
                    List<BackEndTableInfo> backEndTableInfos = schemaBackendMetaMap.get(schemaName).get(tableName);
                    if (calculate.length == 1) {
                        backEndTableInfos1 = Collections.singletonList(backEndTableInfos.get(calculate[0]));
                        break;
                    } else {
                        throw new UnsupportedOperationException();
                    }
                }
            }
        }
        if (backEndTableInfos1 == null) {
            List<ColumnRangeValue> rangeValues = queryDataRange.getRangeValues();
            if (rangeValues != null && !rangeValues.isEmpty()) {
                for (ColumnRangeValue rangeValue : rangeValues) {
                    SQLTableSource tableSource = rangeValue.getTableSource();
                    if (tableSource instanceof SQLExprTableSource) {
                        SQLExprTableSource table = (SQLExprTableSource) tableSource;
                        SchemaObject schemaObject = table.getSchemaObject();
                        schemaName = SQLUtils.normalize(schemaObject.getSchema().getName()).toLowerCase();
                        tableName = SQLUtils.normalize(schemaObject.getName()).toLowerCase();

                        if (fail) {
                            break;
                        }

                        DataMappingEvaluator dataMappingEvaluator = logicTableDataMappingOiginalEvaluator
                                .get(schemaName)
                                .get(tableName)
                                .copy();
                        dataMappingEvaluator.assignmentRange(false, SQLUtils.normalize(rangeValue.getColumn().getColumnName()), Objects.toString(rangeValue.getBegin()), Objects.toString(rangeValue.getEnd()));
                        int[] calculate = dataMappingEvaluator.calculate();
                        List<BackEndTableInfo> backEndTableInfos = schemaBackendMetaMap.get(schemaName).get(tableName);
                        ArrayList<BackEndTableInfo> list = new ArrayList<>();
                        for (int i : calculate) {
                            list.add(backEndTableInfos.get(calculate[i]));
                        }
                        backEndTableInfos1 = list;
                    }
                }
            }
        }

        if (schemaName != null && tableName != null && backEndTableInfos1 == null) {
            backEndTableInfos1 = new ArrayList<>(schemaBackendMetaMap.get(schemaName).get(tableName));
        }
        return new Rrs(new HashSet<>(backEndTableInfos1), schemaName, tableName);
    }

    private void addCreateTableSQL(String schema, String table, String sql) {
        if (sql != null && !sql.isEmpty()) {
            Map<String, String> tableMap = logicTableCreateSQLMap.computeIfAbsent(schema, s -> new HashMap<>());
            tableMap.put(table, sql);
        }
    }


    public BackEndTableInfo getBackEndTableInfo(String schemaName, String tableName, String partitionValue) {
        try {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
            Map<String, DataMappingConfig> dataMappingConfigMap = schemaDataMappingMetaMap.get(schemaName);
            DataMappingConfig dataMappingConfig = dataMappingConfigMap.get(tableName);

            Map<String, List<BackEndTableInfo>> backMap = schemaBackendMetaMap.get(schemaName);
            List<BackEndTableInfo> backEndTableInfos = backMap.get(tableName);
            return backEndTableInfos.get(dataMappingConfig.ruleAlgorithm.calculate(partitionValue));
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new MycatException("{0} {1} {2} can not calculate", schemaName, tableName, partitionValue);
        }
    }


    public List<BackEndTableInfo> getBackEndTableInfo(String schemaName, String tableName, String startValue, String endValue) {
        try {
            Map<String, DataMappingConfig> dataMappingConfigMap = schemaDataMappingMetaMap.get(schemaName);
            DataMappingConfig dataMappingConfig = dataMappingConfigMap.get(tableName);

            Map<String, List<BackEndTableInfo>> backMap = schemaBackendMetaMap.get(schemaName);
            List<BackEndTableInfo> backEndTableInfos = backMap.get(tableName);

            int[] ints = dataMappingConfig.ruleAlgorithm.calculateRange(startValue, endValue);
            List<BackEndTableInfo> res = new ArrayList<>();
            for (int anInt : ints) {
                res.add(backEndTableInfos.get(anInt));
            }
            return res;
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new MycatException("{0} {1} {2} {3} can not calculate", schemaName, tableName, startValue, endValue);
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    @Builder
    @ToString
    static class SchemaInfo {
        String logicSchema;
        String logicTable;
        String targetSchema;
        String targetTable;
    }

    private <T, K, V> void addTableDataMapping(String schemaName, String tableName, List<String> columnList, String rule, Map<String, String> properties, Map<String, String> ranges) {
        final String lowCaseSchemaName = schemaName.toLowerCase();
        final String lowCaseTableName = tableName.toLowerCase();
        final List<String> lowCaseColumnList = columnList.stream().map(i -> i.toLowerCase()).collect(Collectors.toList());
        schemaDataMappingMetaMap.compute(lowCaseSchemaName, (s, stringDataMappingEvaluatorMap) -> {
            if (stringDataMappingEvaluatorMap == null) {
                stringDataMappingEvaluatorMap = new HashMap<>();
            }
            RuleAlgorithm ruleAlgorithm = PartitionRuleAlgorithmManager.INSTANCE.getRuleAlgorithm(rule, properties, ranges);
            stringDataMappingEvaluatorMap.put(lowCaseTableName, new DataMappingConfig(lowCaseColumnList, ruleAlgorithm));
            return stringDataMappingEvaluatorMap;
        });
    }

    public static void addTable(Map<JdbcDataSource, Set<SchemaInfo>> map, JdbcDataSource physical, SchemaInfo schemaInfo) {
        Set<SchemaInfo> schemaInfos = map.computeIfAbsent(physical, s -> new HashSet<>());
        schemaInfos.add(schemaInfo);
    }

    public static JdbcDataSource getPhysical(BackEndTableInfo next) {
        return next.getDatasource(true, null);
    }

    public CalciteConnection getConnection() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false;lex=MYSQL;fun=mysql;conformance=MYSQL_5");
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            rootSchema.setCacheEnabled(true);
            Map<String, Map<String, JdbcTable>> schemaMap = getTableMap();
            schemaMap.forEach((k, v) -> {
                SchemaPlus schemaPlus = rootSchema.add(k, new AbstractSchema());
                v.forEach((t, j) -> {
                    schemaPlus.add(t, j);
                });
            });
            return calciteConnection;
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new RuntimeException(e);
        }
    }

    public synchronized Map<String, Map<String, JdbcTable>> getTableMap() {

        return logicTableMap;
    }

    public void buildLogicTable() {
        schemaBackendMetaMap.forEach((schemaName, tables) -> {
            try {
                ConcurrentHashMap<String, JdbcTable> tableMap;
                logicTableMap.put(schemaName, tableMap = new ConcurrentHashMap<>());
                for (Map.Entry<String, List<BackEndTableInfo>> entry : tables.entrySet()) {
                    String tableName = entry.getKey().toLowerCase();
                    List<BackEndTableInfo> value = entry.getValue();
                    List<SimpleColumnInfo> columnInfos = schemaColumnMetaMap.get(schemaName).get(tableName);
                    RowSignature rowSignature = CalciteConvertors.rowSignature(columnInfos);
                    Optional<DataMappingConfig> optional = Optional.ofNullable(schemaDataMappingMetaMap.get(schemaName)).flatMap(s -> Optional.ofNullable(s.get(tableName)));
                    DataMappingEvaluator dataMappingEvaluator = null;
                    if (optional.isPresent()) {
                        DataMappingConfig dataMappingConfig = optional.get();
                        RuleAlgorithm ruleAlgorithm = dataMappingConfig.ruleAlgorithm;
                        dataMappingEvaluator = new DataMappingEvaluator(rowSignature, dataMappingConfig.columnName, ruleAlgorithm);
                    } else {
                        dataMappingEvaluator = new DataMappingEvaluator(rowSignature);
                    }
                    Map<String, DataMappingEvaluator> dataEvalTableMap = this.logicTableDataMappingOiginalEvaluator.computeIfAbsent(schemaName, s -> new HashMap<>());
                    dataEvalTableMap.put(tableName, dataMappingEvaluator);
                    tableMap.put(tableName, new JdbcTable(schemaName, tableName, value,
                            CalciteConvertors.relDataType(columnInfos), rowSignature, dataMappingEvaluator
                    ));
                    LOGGER.error("build {}.{} success", schemaName, tableName);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }


    private void addSchema(String schemaName) {
        this.schemaBackendMetaMap.put(schemaName.toLowerCase(), new HashMap<>());
    }

    private void addTable(String schemaName, String tableName, List<BackEndTableInfo> tableInfos) {
        Map<String, List<BackEndTableInfo>> map = this.schemaBackendMetaMap.get(schemaName.toLowerCase());
        for (BackEndTableInfo tableInfo : tableInfos) {
            SchemaInfo schemaInfo = tableInfo.getSchemaInfo();
            schemaInfo.logicSchema = schemaName.toLowerCase();
            schemaInfo.logicTable = tableName.toLowerCase();

            schemaInfo.targetSchema = schemaInfo.targetSchema.toLowerCase();
            schemaInfo.targetTable = schemaInfo.targetTable.toLowerCase();
        }

        map.put(tableName, tableInfos);
    }

    private class Rrs {
        Set<BackEndTableInfo> backEndTableInfos;
        String schemaName;
        String tableName;

        public Rrs(Set<BackEndTableInfo> backEndTableInfos, String schemaName, String tableName) {
            this.backEndTableInfos = backEndTableInfos;
            this.schemaName = schemaName.toLowerCase();
            this.tableName = tableName.toLowerCase();
        }

        public Set<BackEndTableInfo> getBackEndTableInfos() {
            return backEndTableInfos;
        }


        public String getSchemaName() {
            return schemaName;
        }


        public String getTableName() {
            return tableName;
        }

    }

    public static void main(String[] args) {

    }
}
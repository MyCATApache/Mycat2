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
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import io.mycat.ConfigRuntime;
import io.mycat.MycatException;
import io.mycat.calcite.shardingQuery.Rrs;
import io.mycat.calcite.shardingQuery.SchemaInfo;
import io.mycat.config.ConfigFile;
import io.mycat.config.shardingQuery.ShardingQueryRootConfig;
import io.mycat.router.RuleAlgorithm;
import io.mycat.router.function.PartitionRuleAlgorithmManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    final ConcurrentHashMap<String, Map<String, String>> logicTableCreateSQLMap = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Map<String, DataMappingEvaluator>> logicTableDataMappingOiginalEvaluator = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Map<String, JdbcTable>> logicTableMap = new ConcurrentHashMap<>();
    public final SchemaRepository TABLE_REPOSITORY = new SchemaRepository(DbType.mysql);

    MetadataManager() {
        PartitionRuleAlgorithmManager.INSTANCE.initFunctions(ConfigRuntime.INSTCANE.load().getConfig(ConfigFile.FUNCTIONS));
        ShardingQueryRootConfig shardingQueryRootConfig = ConfigRuntime.INSTCANE.getConfig(ConfigFile.SHARDING_QUERY);
        if (shardingQueryRootConfig == null) {
            MetadataManagerBuilder.exampleBuild(this);
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
            Map<String, List<BackEndTableInfo>> tableList = entry.getValue();
            HashMap<String, List<BackEndTableInfo>> res = new HashMap<>();
            for (Map.Entry<String, List<BackEndTableInfo>> listEntry : tableList.entrySet()) {
                String tableName = listEntry.getKey();
                for (BackEndTableInfo next : listEntry.getValue()) {
                    SchemaInfo schemaInfo = next.getSchemaInfo();
                    schemaInfo.toLowCase();
                    res.put(tableName, listEntry.getValue());
                    res.put(tableName.toLowerCase(), listEntry.getValue());
                }
            }
            entry.setValue(res);
        }

        if (schemaColumnMetaMap.isEmpty()) {
            schemaColumnMetaMap.putAll(CalciteConvertors.columnInfoListByDataSourceWithCreateTableSQL(schemaBackendMetaMap, this.logicTableCreateSQLMap));
        }
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
                    Map<String, DataMappingEvaluator> dataEvalTableMap = logicTableDataMappingOiginalEvaluator.computeIfAbsent(schemaName, s -> new HashMap<>());
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
        logicTableCreateSQLMap.forEach((schemaName, tableMap) -> {

            tableMap.forEach((table, sql) -> {
                TABLE_REPOSITORY.setDefaultSchema(schemaName);
                TABLE_REPOSITORY.acceptDDL(sql);
            });
        });
    }

    //////////////////////////////////////////////////////function/////////////////////////////////////////////////////
    public Iterator<Map<BackEndTableInfo, String>> getInsertInfoIterator(String currentSchemaNameText, Iterator<MySqlInsertStatement> listIterator) {
      final String  currentSchemaName =  currentSchemaNameText.toLowerCase();
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
                Map<String, DataMappingEvaluator> stringDataMappingEvaluatorMap = logicTableDataMappingOiginalEvaluator.get(schema);
                DataMappingEvaluator dataMappingEvaluator = stringDataMappingEvaluatorMap.get(tableName).copy();
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
                for (Map.Entry<BackEndTableInfo, List<SQLInsertStatement.ValuesClause>> entry : res.entrySet()) {
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
        Map<BackEndTableInfo, String> sqls = new HashMap<>();
        for (BackEndTableInfo endTableInfo : rrs.getBackEndTableInfos()) {
            SchemaInfo schemaInfo = endTableInfo.getSchemaInfo();
            SQLExprTableSource table = rrs.getTable();
            table.setExpr(new SQLPropertyExpr(schemaInfo.getTargetSchema(), schemaInfo.getTargetTable()));
            sqls.put(endTableInfo, SQLUtils.toMySqlString(sqlStatement));
        }
        return sqls;
    }

    //////////////////////////////////////////calculate///////////////////////////////
    public Rrs assignment(boolean fail, QueryDataRange queryDataRange) {
        String schemaName = null;
        String tableName = null;
        SQLExprTableSource table = null;
        if (queryDataRange.getTableSource() != null) {
            table = queryDataRange.getTableSource();
            SchemaObject schemaObject = table.getSchemaObject();
            schemaName = SQLUtils.normalize(schemaObject.getSchema().getName()).toLowerCase();
            tableName = SQLUtils.normalize(schemaObject.getName()).toLowerCase();
        }

        Set<BackEndTableInfo> backEndTableInfos1 = new HashSet<>(1);
        if (queryDataRange != null) {
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

                        DataMappingEvaluator dataMappingEvaluator = logicTableDataMappingOiginalEvaluator
                                .get(schemaName)
                                .get(tableName)
                                .copy();
                        dataMappingEvaluator.assignment(false, equalValue.getColumn().computeAlias(), Objects.toString(equalValue.getValue()));
                        int[] calculate = dataMappingEvaluator.calculate();
                        List<BackEndTableInfo> backEndTableInfos = schemaBackendMetaMap.get(schemaName).get(tableName);
                        if (calculate.length == 1) {
                            backEndTableInfos1 = Collections.singleton(backEndTableInfos.get(calculate[0]));
                            break;
                        } else {

                        }
                    }
                }
            } else if (backEndTableInfos1.isEmpty() || tableName == null) {
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

                            DataMappingEvaluator dataMappingEvaluator = logicTableDataMappingOiginalEvaluator
                                    .get(schemaName)
                                    .get(tableName)
                                    .copy();
                            dataMappingEvaluator.assignmentRange(false, SQLUtils.normalize(rangeValue.getColumn().getColumnName()), Objects.toString(rangeValue.getBegin()), Objects.toString(rangeValue.getEnd()));
                            int[] calculate = dataMappingEvaluator.calculate();
                            List<BackEndTableInfo> backEndTableInfos = schemaBackendMetaMap.get(schemaName).get(tableName);
                            Set<BackEndTableInfo> list = new HashSet<>();
                            for (int i : calculate) {
                                list.add(backEndTableInfos.get(calculate[i]));
                            }
                            backEndTableInfos1 = list;
                        }
                    }
                }
            }
        }
        if (schemaName != null && tableName != null && backEndTableInfos1.isEmpty()) {
            backEndTableInfos1.addAll(schemaBackendMetaMap.get(schemaName).get(tableName));
        }
        return new Rrs(backEndTableInfos1, table);
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

    //////////////////////////////////////////////builder///////////////////////////////////////////////////
    public <T, K, V> void addTableDataMapping(String schemaName, String tableName, List<String> columnList, String rule, Map<String, String> properties, Map<String, String> ranges) {
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

    public void addCreateTableSQL(String schema, String table, String sql) {
        if (sql != null && !sql.isEmpty()) {
            Map<String, String> tableMap = logicTableCreateSQLMap.computeIfAbsent(schema.toLowerCase(), s -> new HashMap<>());
            tableMap.put(table.toLowerCase(), sql);
        }
    }

    public void addSchema(String schemaName) {
        this.schemaBackendMetaMap.put(schemaName.toLowerCase(), new HashMap<>());
    }

    public void addTable(String schemaName, String tableName, List<BackEndTableInfo> tableInfos) {
        schemaName = schemaName.toLowerCase();
        tableName = tableName.toLowerCase();
        Map<String, List<BackEndTableInfo>> map = this.schemaBackendMetaMap.get(schemaName);
        for (BackEndTableInfo tableInfo : tableInfos) {
            SchemaInfo schemaInfo = tableInfo.getSchemaInfo();
            schemaInfo.setLogicSchema(schemaName);
            schemaInfo.setLogicTable(tableName);
            schemaInfo.toLowCase();
        }
        map.put(tableName, tableInfos);
    }
}
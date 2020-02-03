package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.BackendTableInfo;
import io.mycat.QueryBackendTask;
import io.mycat.SchemaInfo;
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CalciteUtls {
    private final static Logger LOGGER = LoggerFactory.getLogger(CalciteUtls.class);

    public static List<QueryBackendTask> getQueryBackendTasks(MetadataManager.LogicTable table, List<RexNode> filters, int[] projects) {
        List<BackendTableInfo> calculate = getBackendTableInfos(table, filters);


        //
        List<SimpleColumnInfo> rawColumnList = table.getRawColumns();
        List<SimpleColumnInfo> projectColumnList = getColumnList(table, projects);
        List<QueryBackendTask> list = new ArrayList<>();
        for (BackendTableInfo backendTableInfo : calculate) {
            String backendTaskSQL = getBackendTaskSQL(filters, rawColumnList, projectColumnList, backendTableInfo);
            QueryBackendTask queryBackendTask = new QueryBackendTask(backendTaskSQL, backendTableInfo.getTargetName());
            list.add(queryBackendTask);
        }
        return list;

    }

    public static List<BackendTableInfo> getBackendTableInfos(MetadataManager.LogicTable table, List<RexNode> filters) {
        LOGGER.info("origin  filters:{}", filters);
        DataMappingEvaluator record = new DataMappingEvaluator();
        ArrayList<RexNode> where = new ArrayList<>();
        if (table.isNatureTable()) {
            filters.removeIf((filter) -> {
                DataMappingEvaluator dataMappingRule = new DataMappingEvaluator();
                boolean success = addOrRootFilter(table, dataMappingRule, filter);
                if (success) {
                    record.merge(dataMappingRule);
                }
                where.add(filter);
                return success;
            });
        } else {
            filters.forEach((filter) -> {
                DataMappingEvaluator dataMappingRule = new DataMappingEvaluator();
                boolean success = addOrRootFilter(table, dataMappingRule, filter);
                if (success) {
                    record.merge(dataMappingRule);
                }
                where.add(filter);
            });
        }

        LOGGER.info("optimize filters:{}", filters);
        return record.calculate(table);
    }

    @NotNull
    private static String getBackendTaskSQL(List<RexNode> filters, List<SimpleColumnInfo> rawColumnList, List<SimpleColumnInfo> projectColumnList, BackendTableInfo backendTableInfo) {
        SchemaInfo schemaInfo = backendTableInfo.getSchemaInfo();
        String targetSchema = schemaInfo.getTargetSchema();
        String targetTable = schemaInfo.getTargetTable();
        String targetSchemaTable = schemaInfo.getTargetSchemaTable();
        return getBackendTaskSQL(filters, rawColumnList, projectColumnList, targetSchema, targetTable, targetSchemaTable);
    }

    public static String getBackendTaskSQL( MetadataManager.LogicTable table,BackendTableInfo backendTableInfo, int[] projects, List<RexNode> filters) {
        List<SimpleColumnInfo> rawColumnList = table.getRawColumns();
        List<SimpleColumnInfo> projectColumnList = getColumnList(table, projects);
        return getBackendTaskSQL(filters,rawColumnList,projectColumnList,backendTableInfo);
    }

    public static String getBackendTaskSQL(List<RexNode> filters, List<SimpleColumnInfo> rawColumnList, List<SimpleColumnInfo> projectColumnList, String targetSchema, String targetTable, String targetSchemaTable) {
        StringBuilder sqlBuilder = new StringBuilder();
        String selectItems = projectColumnList.isEmpty() ? "*" : projectColumnList.stream().map(i -> i.getColumnName()).map(i -> targetSchemaTable + "." + i).collect(Collectors.joining(","));
        sqlBuilder.append(MessageFormat.format("select {0} from {1}", selectItems, targetSchemaTable));
        sqlBuilder.append(getFilterSQLText(rawColumnList, targetSchema, targetTable, filters));
        return sqlBuilder.toString();
    }

    public static List<SimpleColumnInfo> getColumnList(MetadataManager.LogicTable table, final int[] projects) {
        if (projects == null) {
            return Collections.emptyList();
        } else {
            List<SimpleColumnInfo> rawColumns = table.getRawColumns();
            return Arrays.stream(projects).mapToObj(rawColumns::get).collect(Collectors.toList());
        }
    }

    public static String getFilterSQLText(List<SimpleColumnInfo> rawColumns, String schemaName, String tableName, List<RexNode> filters) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        SqlImplementor.Context context = new SqlImplementor.Context(MysqlSqlDialect.DEFAULT, rawColumns.size()) {
            @Override
            public SqlNode field(int ordinal) {
                String fieldName = rawColumns.get(ordinal).getColumnName();
                return new SqlIdentifier(ImmutableList.of(schemaName, tableName, fieldName),
                        SqlImplementor.POS);
            }
        };
        return filters.stream().map(i -> context.toSql(null, i).toSqlString(MysqlSqlDialect.DEFAULT))
                .map(i -> i.getSql())
                .collect(Collectors.joining(" and ", " where ", ""));
    }

    public static boolean addOrRootFilter(MetadataManager.LogicTable table, DataMappingEvaluator evaluator, RexNode filter) {
        if (filter.isA(SqlKind.OR)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            int size = operands.size();
            int count = 0;
            for (RexNode operand : operands) {
                if (addFilter(table, evaluator, operand, true)) {
                    ++count;
                }
            }
            return count == size;
        }
        return addFilter(table, evaluator, filter, false);
    }

    public static boolean addFilter(MetadataManager.LogicTable table, DataMappingEvaluator evaluator, RexNode filter, boolean or) {
        List<SimpleColumnInfo> rowOrder = table.getRawColumns();
        if (filter.isA(SqlKind.AND)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            int size = operands.size();
            boolean[] trueList = new boolean[size];
            for (int i = 0, j = 1; i < size && j < size; i++, j++) {
                RexNode left = operands.get(i);
                RexNode right = operands.get(j);
                if (left instanceof RexCall && right instanceof RexCall) {
                    if (left.isA(SqlKind.GREATER_THAN_OR_EQUAL) && right.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
                        RexNode fisrtExpr = ((RexCall) left).getOperands().get(0);
                        RexNode secondExpr = ((RexCall) right).getOperands().get(0);
                        if (fisrtExpr instanceof RexInputRef && secondExpr instanceof RexInputRef) {
                            int index = ((RexInputRef) fisrtExpr).getIndex();
                            if (index == ((RexInputRef) secondExpr).getIndex()) {
                                RexNode start = ((RexCall) left).getOperands().get(1);
                                RexNode end = ((RexCall) right).getOperands().get(1);
                                if (start instanceof RexLiteral && end instanceof RexLiteral) {
                                    String startValue = ((RexLiteral) start).getValue2().toString();
                                    String endValue = ((RexLiteral) end).getValue2().toString();
                                    evaluator.assignmentRange(or, rowOrder.get(index).getColumnName(), startValue, endValue);
                                    trueList[i] = trueList[i] || true;
                                    trueList[j] = trueList[j] || true;
                                }
                            }
                        }
                    }
                }
                for (int k = 0; k < size; k++) {
                    if (!trueList[k]) {
                        if (!addFilter(table, evaluator, operands.get(k), or)) {
                            return false;
                        }
                    }
                }
                return true;
            }
            return false;
        } else if (filter.isA(SqlKind.EQUALS)) {
            RexCall call = (RexCall) filter;
            RexNode left = (RexNode) call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = (RexNode) ((RexCall) left).operands.get(0);
            }
            RexNode right = (RexNode) call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                String value = ((RexLiteral) right).getValue2().toString();
                evaluator.assignment(or, rowOrder.get(index).getColumnName(), value);
                return true;
            }

        }
        return false;
    }


    public static AtomicBoolean getCancelFlag(DataContext root) {
        AtomicBoolean tempFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        if (tempFlag == null) {
            return new AtomicBoolean(false);
        } else {
            return tempFlag;
        }
    }
}
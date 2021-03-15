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
package io.mycat.util;

import com.google.common.collect.ImmutableList;
import io.mycat.*;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.querycondition.DataMappingEvaluator;
import io.mycat.router.ShardingTableHandler;
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public class CalciteUtls {
    private final static Logger LOGGER = LoggerFactory.getLogger(CalciteUtls.class);

    public static List<QueryBackendTask> getQueryBackendTasks(ShardingTableHandler table, List<RexNode> filters, int[] projects) {
        List<DataNode> calculate = getBackendTableInfos(table, filters);


        //
        List<SimpleColumnInfo> rawColumnList = table.getColumns();
        List<SimpleColumnInfo> projectColumnList = getColumnList(table, projects);
        List<QueryBackendTask> list = new ArrayList<>();
        for (DataNode backendTableInfo : calculate) {
            String targetName = backendTableInfo.getTargetName();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE
                    .getSqlDialectByTargetName(targetName);
            String backendTaskSQL = getBackendTaskSQL(dialect, filters, rawColumnList, projectColumnList, backendTableInfo);
            QueryBackendTask queryBackendTask = new QueryBackendTask(backendTableInfo.getTargetName(), backendTaskSQL);
            list.add(queryBackendTask);
        }
        return list;

    }

    public static void collect(Union e, List<RelNode> unions) {
        for (RelNode input : e.getInputs()) {
            if (input instanceof Union) {
                collect((Union) input, unions);
            } else {
                unions.add(input);
            }
        }
    }

    public static List<DataNode> getBackendTableInfos(ShardingTableHandler table, List<RexNode> filters) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("origin  filters:{}", filters);
        }
        DataMappingEvaluator record = new DataMappingEvaluator();
        for (RexNode filter : filters) {
            DataMappingEvaluator dataMappingRule = new DataMappingEvaluator();
            boolean success = addOrRootFilter(table, dataMappingRule, filter);
            if (success) {
                record.merge(dataMappingRule);
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("optimize filters:{}", filters);
        }
        List<DataNode> dataNodeList = table.function().calculate(record.getColumnMap());
        return dataNodeList;
    }

    @NotNull
    public static String getBackendTaskSQL(SqlDialect dialect,
                                           List<RexNode> filters,
                                           List<SimpleColumnInfo> rawColumnList,
                                           List<SimpleColumnInfo> projectColumnList,
                                           DataNode backendTableInfo) {
        String targetSchema = backendTableInfo.getSchema();
        String targetTable = backendTableInfo.getTable();
        String targetSchemaTable = backendTableInfo.getTargetSchemaTable();
        return getBackendTaskSQL(dialect, filters, rawColumnList, projectColumnList, targetSchema, targetTable, targetSchemaTable);
    }

    public static String getBackendTaskSQL(SqlDialect dialect, ShardingTableHandler table, BackendTableInfo backendTableInfo, int[] projects, List<RexNode> filters) {
        List<SimpleColumnInfo> rawColumnList = table.getColumns();
        List<SimpleColumnInfo> projectColumnList = getColumnList(table, projects);
        return getBackendTaskSQL(dialect, filters, rawColumnList, projectColumnList, backendTableInfo);
    }

    public static String getBackendTaskSQL(
            SqlDialect dialect,
            List<RexNode> filters, List<SimpleColumnInfo> rawColumnList,
            List<SimpleColumnInfo> projectColumnList,
            String targetSchema,
            String targetTable,
            String targetSchemaTable) {
        StringBuilder sqlBuilder = new StringBuilder();
        String selectItems = projectColumnList.isEmpty() ? "*" : projectColumnList.stream().map(i -> i.getColumnName()).map(i -> targetSchemaTable + "." + i).collect(Collectors.joining(","));
        sqlBuilder.append(MessageFormat.format("select {0} from {1} ", selectItems, targetSchemaTable));
        sqlBuilder.append(getFilterSQLText(dialect, rawColumnList, targetSchema, targetTable, filters));
        return sqlBuilder.toString();
    }

    public static List<SimpleColumnInfo> getColumnList(TableHandler table, final int[] projects) {
        if (projects == null) {
            return Collections.emptyList();
        } else {
            List<SimpleColumnInfo> rawColumns = table.getColumns();
            return Arrays.stream(projects).mapToObj(rawColumns::get).collect(Collectors.toList());
        }
    }

    public static String getFilterSQLText(SqlDialect dialect,
                                          List<SimpleColumnInfo> rawColumns,
                                          String schemaName,
                                          String tableName,
                                          List<RexNode> filters) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        RexNode rexNode = RexUtil.composeConjunction(MycatCalciteSupport.INSTANCE.RexBuilder, filters);
        SqlImplementor.Context context = new SqlImplementor.Context(dialect, rawColumns.size()) {
            @Override
            public SqlNode field(int ordinal) {
                String fieldName = rawColumns.get(ordinal).getColumnName();
                return new SqlIdentifier(ImmutableList.of(schemaName, tableName, fieldName),
                        SqlImplementor.POS);
            }

            @Override
            public SqlImplementor implementor() {
                return null;
            }
        };
        try {
            return " where " + context.toSql(null, rexNode).toSqlString(dialect).getSql();
        } catch (Exception e) {
            LOGGER.warn("不能生成对应的sql", e);
        }
        return "";
    }

    public static boolean addOrRootFilter(ShardingTableHandler table, DataMappingEvaluator evaluator, RexNode filter) {
        if (filter.isA(SqlKind.OR)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            int size = operands.size();
            int count = 0;
            for (RexNode operand : operands) {
                if (addFilter(table, evaluator, operand)) {
                    ++count;
                }
            }
            return count == size;
        }
        return addFilter(table, evaluator, filter);
    }

    /**
     * SELECT * FROM travelrecord WHERE id >= 1 AND id <= 10;
     * SELECT * FROM travelrecord WHERE id not in (1,2);
     * SELECT * FROM travelrecord WHERE id in (1,15);
     * SELECT * FROM travelrecord WHERE id not BETWEEN 1 and 2;
     * SELECT * FROM travelrecord WHERE id BETWEEN 1 and 2;
     * SELECT * FROM travelrecord WHERE id BETWEEN 1 and 15;
     * SELECT * FROM travelrecord WHERE
     * (id < 10 AND (id = 3 or true)) OR
     * (id = 15 AND false) OR
     * (id = 15 AND true);
     * SELECT * FROM travelrecord WHERE
     * (id < 10 AND ((id/2) =0 OR id = 3)) OR
     * (id < 100 AND days = 1) OR
     * (id < 100 AND traveldate = '2020-08-22');
     *
     * @param table
     * @param evaluator
     * @param filter
     * @return
     */
    public static boolean addFilter(ShardingTableHandler table, DataMappingEvaluator evaluator, RexNode filter) {
        List<SimpleColumnInfo> rowOrder = table.getColumns();
        if (filter.isA(SqlKind.AND)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            int size = operands.size();
            boolean[] trueList = new boolean[size];
            for (int i = 0, j = 1; i < size && j < size; i++, j++) {
                RexNode left = operands.get(i);
                RexNode right = operands.get(j);
                if (left instanceof RexCall && right instanceof RexCall) {
                    if ((left.isA(SqlKind.GREATER_THAN_OR_EQUAL) || left.isA(SqlKind.GREATER_THAN)) && (right.isA(SqlKind.LESS_THAN_OR_EQUAL) || right.isA(SqlKind.LESS_THAN))) {
                        RexNode fisrtExpr = unCastWrapper(((RexCall) left).getOperands().get(0));
                        RexNode secondExpr = unCastWrapper(((RexCall) right).getOperands().get(0));
                        if (fisrtExpr instanceof RexInputRef && secondExpr instanceof RexInputRef) {
                            int index = ((RexInputRef) fisrtExpr).getIndex();
                            if (index == ((RexInputRef) secondExpr).getIndex()) {
                                RexNode start = unCastWrapper(((RexCall) left).getOperands().get(1));
                                RexNode end = unCastWrapper(((RexCall) right).getOperands().get(1));
                                if (start instanceof RexLiteral && end instanceof RexLiteral) {
                                    Object startValue = ((RexLiteral) start).getValue2();
                                    Object endValue = ((RexLiteral) end).getValue2();
                                    evaluator.assignmentRange(rowOrder.get(index).getColumnName(), startValue, endValue);
                                    trueList[i] = trueList[i] || true;
                                    trueList[j] = trueList[j] || true;
                                }
                            }
                        }
                    }
                }
                for (int k = 0; k < size; k++) {
                    if (!trueList[k]) {
                        if (!addFilter(table, evaluator, operands.get(k))) {
                            return false;
                        }
                    }
                }
                return true;
            }
            return false;
        } else if (filter.isA(SqlKind.EQUALS)) {
            RexCall call = (RexCall) filter;

            RexNode left = call.getOperands().get(0);
            left = unCastWrapper(left);

            RexNode right = call.getOperands().get(1);
            right = unCastWrapper(right);

            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                evaluator.assignment(rowOrder.get(index).getColumnName(),  ((RexLiteral) right).getValue2());
                return true;
            }
        } else if (filter.isA(SqlKind.GREATER_THAN) || filter.isA(SqlKind.LESS_THAN)
                || filter.isA(SqlKind.LESS_THAN_OR_EQUAL) || filter.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
            //这里处理[大于,小于,大于等于,小于等于]的情况.
            RexCall call = (RexCall) filter;

            RexNode left = call.getOperands().get(0);
            left = unCastWrapper(left);

            RexNode right = call.getOperands().get(1);
            right = unCastWrapper(right);

            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                Object value = ((RexLiteral) right).getValue2();
                String columnName = rowOrder.get(index).getColumnName();
                if (filter.isA(SqlKind.GREATER_THAN)) {
                    evaluator.assignmentRange(columnName, value, null);
                    return true;
                } else if (filter.isA(SqlKind.LESS_THAN)) {
                    evaluator.assignmentRange(columnName, null, value);
                    return true;
                } else if (filter.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
                    evaluator.assignmentRange(columnName, value, null);
                    evaluator.assignment(columnName, value);
                    return true;
                } else if (filter.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
                    evaluator.assignmentRange(columnName, null, value);
                    evaluator.assignment(columnName, value);
                    return true;
                }
            }
        } else if (filter.isA(SqlKind.OR)) {
            //这里处理IN的情况，IN会转成多个OR. 例如： id in(1,2,3) 等同于 OR id = 1 or id = 2 or id = 3;
            return addOrRootFilter(table, evaluator, filter);
        } else {
            return false;
        }
        return false;
    }

    public static RexNode unCastWrapper(RexNode left) {
        while (left.isA(SqlKind.CAST)) {
            left = ((RexCall) left).operands.get(0);
        }
        return left;
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
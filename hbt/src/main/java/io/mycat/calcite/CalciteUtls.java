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

import com.google.common.collect.ImmutableList;
import io.mycat.BackendTableInfo;
import io.mycat.QueryBackendTask;
import io.mycat.SchemaInfo;
import io.mycat.metadata.ShardingTableHandler;
import io.mycat.metadata.TableHandler;
import io.mycat.queryCondition.DataMappingEvaluator;
import io.mycat.queryCondition.SimpleColumnInfo;
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.*;
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

/**
 * @author Junwen Chen
 **/
public class CalciteUtls {
    private final static Logger LOGGER = LoggerFactory.getLogger(CalciteUtls.class);

    public static List<QueryBackendTask> getQueryBackendTasks(ShardingTableHandler table, List<RexNode> filters, int[] projects) {
        List<BackendTableInfo> calculate = getBackendTableInfos(table, filters);


        //
        List<SimpleColumnInfo> rawColumnList = table.getColumns();
        List<SimpleColumnInfo> projectColumnList = getColumnList(table, projects);
        List<QueryBackendTask> list = new ArrayList<>();
        for (BackendTableInfo backendTableInfo : calculate) {
            String backendTaskSQL = getBackendTaskSQL(filters, rawColumnList, projectColumnList, backendTableInfo);
            QueryBackendTask queryBackendTask = new QueryBackendTask( backendTableInfo.getTargetName(),backendTaskSQL);
            list.add(queryBackendTask);
        }
        return list;

    }

    public static List<BackendTableInfo> getBackendTableInfos(ShardingTableHandler table, List<RexNode> filters) {
        LOGGER.info("origin  filters:{}", filters);
        DataMappingEvaluator record = new DataMappingEvaluator();
        ArrayList<RexNode> where = new ArrayList<>();
        filters.forEach((filter) -> {
            DataMappingEvaluator dataMappingRule = new DataMappingEvaluator();
            boolean success = addOrRootFilter(table, dataMappingRule, filter);
            if (success) {
                record.merge(dataMappingRule);
            }
            where.add(filter);
        });

        LOGGER.info("optimize filters:{}", filters);
        return record.calculate(table);
    }

    @NotNull
    public static String getBackendTaskSQL(List<RexNode> filters, List<SimpleColumnInfo> rawColumnList, List<SimpleColumnInfo> projectColumnList, BackendTableInfo backendTableInfo) {
        SchemaInfo schemaInfo = backendTableInfo.getSchemaInfo();
        String targetSchema = schemaInfo.getTargetSchema();
        String targetTable = schemaInfo.getTargetTable();
        String targetSchemaTable = schemaInfo.getTargetSchemaTable();
        return getBackendTaskSQL(filters, rawColumnList, projectColumnList, targetSchema, targetTable, targetSchemaTable);
    }

    public static String getBackendTaskSQL(ShardingTableHandler table, BackendTableInfo backendTableInfo, int[] projects, List<RexNode> filters) {
        List<SimpleColumnInfo> rawColumnList = table.getColumns();
        List<SimpleColumnInfo> projectColumnList = getColumnList(table, projects);
        return getBackendTaskSQL(filters, rawColumnList, projectColumnList, backendTableInfo);
    }

    public static String getBackendTaskSQL(List<RexNode> filters, List<SimpleColumnInfo> rawColumnList,
                                           List<SimpleColumnInfo> projectColumnList,
                                           String targetSchema,
                                           String targetTable,
                                           String targetSchemaTable) {
        StringBuilder sqlBuilder = new StringBuilder();
        String selectItems = projectColumnList.isEmpty() ? "*" : projectColumnList.stream().map(i -> i.getColumnName()).map(i -> targetSchemaTable + "." + i).collect(Collectors.joining(","));
        sqlBuilder.append(MessageFormat.format("select {0} from {1} ", selectItems, targetSchemaTable));
        sqlBuilder.append(getFilterSQLText(rawColumnList, targetSchema, targetTable, filters));
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

    public static String getFilterSQLText(List<SimpleColumnInfo> rawColumns, String schemaName, String tableName, List<RexNode> filters) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        RexNode rexNode = RexUtil.composeConjunction(MycatCalciteSupport.INSTANCE.RexBuilder, filters);
        SqlImplementor.Context context = new SqlImplementor.Context(MysqlSqlDialect.DEFAULT, rawColumns.size()) {
            @Override
            public SqlNode field(int ordinal) {
                String fieldName = rawColumns.get(ordinal).getColumnName();
                return new SqlIdentifier(ImmutableList.of(schemaName, tableName, fieldName),
                        SqlImplementor.POS);
            }
        };
        try {
            return " where " + context.toSql(null, rexNode).toSqlString(MysqlSqlDialect.DEFAULT).getSql();
        }catch (Exception e){
            LOGGER.warn("不能生成对应的sql",e);
        }
        return "";
    }

    public static boolean addOrRootFilter(ShardingTableHandler table, DataMappingEvaluator evaluator, RexNode filter) {
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

    public static boolean addFilter(ShardingTableHandler table, DataMappingEvaluator evaluator, RexNode filter, boolean or) {
        List<SimpleColumnInfo> rowOrder = table.getColumns();
        if (filter.isA(SqlKind.AND)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            int size = operands.size();
            boolean[] trueList = new boolean[size];
            for (int i = 0, j = 1; i < size && j < size; i++, j++) {
                RexNode left = operands.get(i);
                RexNode right = operands.get(j);
                if (left instanceof RexCall && right instanceof RexCall) {
                    if ((left.isA(SqlKind.GREATER_THAN_OR_EQUAL)||left.isA(SqlKind.GREATER_THAN))  && (right.isA(SqlKind.LESS_THAN_OR_EQUAL)||right.isA(SqlKind.LESS_THAN))) {
                        RexNode fisrtExpr = unCastWrapper(((RexCall) left).getOperands().get(0));
                        RexNode secondExpr =unCastWrapper(((RexCall) right).getOperands().get(0));
                        if (fisrtExpr instanceof RexInputRef && secondExpr instanceof RexInputRef) {
                            int index = ((RexInputRef) fisrtExpr).getIndex();
                            if (index == ((RexInputRef) secondExpr).getIndex()) {
                                RexNode start =unCastWrapper( ((RexCall) left).getOperands().get(1));
                                RexNode end = unCastWrapper(((RexCall) right).getOperands().get(1));
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
            RexNode left = call.getOperands().get(0);

            left = unCastWrapper(left);



  


            RexNode right = call.getOperands().get(1);
            right = unCastWrapper(right);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                String value = ((RexLiteral) right).getValue2().toString();
                evaluator.assignment(or, rowOrder.get(index).getColumnName(), value);
                return true;
            }

        }
        return false;
    }

    private static RexNode unCastWrapper(RexNode left) {
        if (left.isA(SqlKind.CAST)) {
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
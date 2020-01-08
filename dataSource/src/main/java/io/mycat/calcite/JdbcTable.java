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
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class JdbcTable implements TranslatableTable, ProjectableFilterableTable {
    private MetadataManager.LogicTable table;
    private final RelProtoDataType relProtoDataType;
    private final RowSignature rowSignature;
    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcTable.class);
    public JdbcTable(MetadataManager.LogicTable table, RelProtoDataType relProtoDataType, RowSignature rowSignature) {
        this.table = table;
        this.relProtoDataType = relProtoDataType;
        this.rowSignature = rowSignature;

    }

    private boolean addFilter(DataMappingEvaluator evaluator, RexNode filter, boolean or) {
        List<String> rowOrder = rowSignature.getRowOrder();
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
                                    evaluator.assignmentRange(or, rowOrder.get(index), startValue, endValue);
                                    trueList[i] = trueList[i] || true;
                                    trueList[j] = trueList[j] || true;
                                }
                            }
                        }
                    }
                }
                for (int k = 0; k < size; k++) {
                    if (!trueList[k]) {
                        if (!addFilter(evaluator, operands.get(k), or)) {

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
                evaluator.assignment(or, rowOrder.get(index), value);
                return true;
            }

        }
        return false;
    }

    private boolean addOrRootFilter(DataMappingEvaluator evaluator, RexNode filter) {
        if (filter.isA(SqlKind.OR)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            int size = operands.size();
            int count = 0;
            for (RexNode operand : operands) {
                if (addFilter(evaluator, operand, true)) {
                    ++count;
                }
            }
            return count == size;
        }
        return addFilter(evaluator, filter, false);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        LogicalTableScan logicalTableScan = LogicalTableScan.create(toRelContext.getCluster(), relOptTable);
//        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(MysqlSqlDialect.DEFAULT);
//        SqlImplementor.Result visit = relToSqlConverter.visitChild(0, logicalTableScan);
//        SqlNode sqlNode = visit.asStatement();
//        System.out.println(sqlNode);
        return logicalTableScan;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return this.rowSignature.getRelDataType(typeFactory);

    }


    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    /**
     * @param column
     * @return default false
     */
    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    /**
     * @param column
     * @param call
     * @param parent
     * @param config
     * @return default false
     */
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        return false;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, final int[] projects) {
        List<QueryBackendTask> backendTasks = getQueryBackendTasks(filters, projects);
        return new MyCatResultSetEnumerable(getCancelFlag(root), backendTasks);
    }

    public List<QueryBackendTask> getQueryBackendTasks(List<RexNode> filters, int[] projects) {
        LOGGER.info("origin  filters:{}", filters);
        DataMappingEvaluator record = new DataMappingEvaluator();
        ArrayList<RexNode> where  = new ArrayList<>();
        if(this.table.isNatureTable()){
            filters.removeIf((filter) -> {
                DataMappingEvaluator dataMappingRule = new DataMappingEvaluator();
                boolean success = addOrRootFilter(dataMappingRule, filter);
                if (success) {
                    record.merge(dataMappingRule);
                    where.add(filter);
                }
                return success;
            });
        }else {
            filters.forEach((filter) -> {
                DataMappingEvaluator dataMappingRule = new DataMappingEvaluator();
                boolean success = addOrRootFilter(dataMappingRule, filter);
                if (success) {
                    record.merge(dataMappingRule);
                    where.add(filter);
                }
            });
        }

        LOGGER.info("optimize filters:{}", filters);
        List<BackendTableInfo> calculate = record.calculate(table);
        return getBackendTasks(getColumnList(projects), where, calculate);
    }

    private List<QueryBackendTask> getBackendTasks(List<String> columnList , List<RexNode> filters, List<BackendTableInfo> calculate) {
        List<QueryBackendTask> res = new ArrayList<>();
        for (BackendTableInfo backendTableInfo : calculate) {
            SchemaInfo schemaInfo = backendTableInfo.getSchemaInfo();
            String targetSchemaTable = schemaInfo.getTargetSchemaTable();
            StringBuilder sql = new StringBuilder();
            String selectItems = columnList.stream().map(i -> targetSchemaTable + "." + i).collect(Collectors.joining(","));
            sql.append(MessageFormat.format("select {0} from {1}", selectItems, targetSchemaTable));
            sql.append(getFilterSQLText(schemaInfo.getTargetSchema(), schemaInfo.getTargetTable(), filters));
            res.add(new QueryBackendTask(sql.toString(), backendTableInfo));
        }
        return res;
    }

    private List<String> getColumnList(final int[] projects) {
        List<String> rowOrder = this.rowSignature.getRowOrder();
        if (projects == null) {
            return rowOrder;
        } else {
            return Arrays.stream(projects).mapToObj(rowOrder::get).collect(Collectors.toList());
        }
    }

    private String getFilterSQLText(String schemaName, String tableName, List<RexNode> filters) {
        if (filters==null||filters.isEmpty()){
            return "";
        }
        SqlImplementor.Context context = new SqlImplementor.Context(MysqlSqlDialect.DEFAULT, JdbcTable.this.rowSignature.getColumnCount()) {
            @Override
            public SqlNode field(int ordinal) {
                String fieldName = JdbcTable.this.rowSignature.getRowOrder().get(ordinal);
                return new SqlIdentifier(ImmutableList.of(schemaName, tableName, fieldName),
                        SqlImplementor.POS);
            }
        };

        return filters.stream().map(i -> context.toSql(null, i).toSqlString(MysqlSqlDialect.DEFAULT))
                .map(i -> i.getSql())
                .collect(Collectors.joining(" and ", " where ", ""));
    }

    private AtomicBoolean getCancelFlag(DataContext root) {
        AtomicBoolean tempFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        if (tempFlag == null) {
            return new AtomicBoolean(false);
        } else {
            return tempFlag;
        }
    }
}
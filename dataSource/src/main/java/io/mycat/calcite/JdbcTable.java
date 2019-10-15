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

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class JdbcTable implements TranslatableTable, ProjectableFilterableTable {
    private final String schemaName;
    private final String tableName;
    private final RelProtoDataType relProtoDataType;
    private final RowSignature rowSignature;
    private final DataMappingEvaluator originaldataMappingRule;
    private final List<BackendTableInfo> backStoreList;
    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcTable.class);

    public JdbcTable(String schemaName, String tableName, List<BackendTableInfo> value, RelProtoDataType relProtoDataType, RowSignature rowSignature, DataMappingEvaluator dataMappingRule) {
        Objects.requireNonNull(value);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.backStoreList = value;
        this.relProtoDataType = relProtoDataType;
        this.rowSignature = rowSignature;
        this.originaldataMappingRule = dataMappingRule;
    }

    private static boolean addFilter(DataMappingEvaluator evaluator, RexNode filter, boolean or) {
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
                                    evaluator.assignmentRange(or, index, startValue, endValue);
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
                evaluator.fail = false;
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
                evaluator.assignment(or, index, value);
                evaluator.fail = false;
                return true;
            }

        }
        return false;
    }

    private static boolean addOrRootFilter(DataMappingEvaluator evaluator, RexNode filter) {
        evaluator.fail = true;
        if (filter.isA(SqlKind.OR)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            int size = operands.size();
            int i = 0;
            for (; i < size; i++) {
                if (addFilter(evaluator, operands.get(i), true)) {
                    continue;
                }
                break;
            }
            evaluator.fail = i != size;
            return !evaluator.fail;
        }
        return addFilter(evaluator, filter, false);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        return LogicalTableScan.create(toRelContext.getCluster(), relOptTable);
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        try {
            return this.rowSignature.getRelDataType(typeFactory);
        } catch (Exception e) {
            LOGGER.error("", e);
            return null;
        }
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
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        LOGGER.info("origin  filters:{}", filters);
        DataMappingEvaluator record = this.originaldataMappingRule.copy();
        record.fail = false;
        String filterText = "";
        if (!filters.isEmpty()) {
            filters.removeIf((filter) -> {
                DataMappingEvaluator dataMappingRule = this.originaldataMappingRule.copy();
                dataMappingRule.fail = true;
                boolean b = addOrRootFilter(dataMappingRule, filter);
                if (!dataMappingRule.fail) {
                    record.add(dataMappingRule);
                }
                return b;
            });
            filterText = record.getFilterExpr();
        }
        LOGGER.info("optimize filters:{}", filters);
        List<BackendTableInfo> backStoreList = this.backStoreList;
        int[] calculate = record.calculate();
        if (calculate.length == 0) {
            backStoreList = this.backStoreList;
        }
        if (calculate.length == 1) {
            if (calculate[0] == -1) {
                backStoreList = this.backStoreList;
            } else {
                backStoreList = Collections.singletonList(this.backStoreList.get(calculate[0]));
            }
        }
        if (calculate.length > 1) {
            backStoreList = new ArrayList<>(calculate.length);
            int size = this.backStoreList.size();
            for (int i1 : calculate) {
                if (i1 >= size || i1 == -1) {
                    backStoreList = this.backStoreList;
                    break;
                } else {
                    backStoreList.add(this.backStoreList.get(i1));
                }
            }
        }
         AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        if (cancelFlag == null){
            cancelFlag = new AtomicBoolean(false);
        }
        if (projects == null) {
            return new MyCatResultSetEnumerable(cancelFlag, backStoreList, "*", filterText);
        } else {
            StringBuilder projectText = new StringBuilder();
            List<String> rowOrder = rowSignature.getRowOrder();
            for (int i = 0; i < projects.length; i++) {
                if (i == 0) {
                    projectText.append(rowOrder.get(projects[i]));
                } else {
                    projectText.append(",").append(rowOrder.get(projects[i]));
                }
            }
            return new MyCatResultSetEnumerable(cancelFlag, backStoreList, projectText.toString(), filterText);
        }
    }

    public List<BackendTableInfo> getBackStoreList() {
        return backStoreList;
    }
}
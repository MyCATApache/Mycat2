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
import java.util.concurrent.atomic.AtomicBoolean;

public class JdbcTable implements TranslatableTable, ProjectableFilterableTable {
    private final String schemaName;
    private final String tableName;
    private final RelProtoDataType relProtoDataType;
    private final RowSignature rowSignature;
    private final DataMappingEvaluator dataMappingRule;
    private final List<BackEndTableInfo> backStoreList;
    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcTable.class);

    public JdbcTable(String schemaName, String tableName, List<BackEndTableInfo> value, RelProtoDataType relProtoDataType, RowSignature rowSignature, DataMappingEvaluator dataMappingRule) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.backStoreList = value;
        this.relProtoDataType = relProtoDataType;
        this.rowSignature = rowSignature;
        this.dataMappingRule = dataMappingRule;
    }

    private boolean addFilter(RexNode filter) {
        if (filter.isA(SqlKind.AND)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            if (operands.size() == 2) {
                RexNode left = operands.get(0);
                RexNode right = operands.get(1);
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
                                    dataMappingRule.assignmentRange(false, index, startValue, endValue);
                                    return true;
                                }
                            }
                        }
                    }
                }
            } else {
                for (RexNode operand : operands) {
                    if (!addFilter(operand)) {
                        return false;
                    }
                }
                return true;
            }
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
                return dataMappingRule.assignment(false, index, value);
            }
        }
        return false;
    }

    private boolean addOrFilter(RexNode filter) {
        if (filter.isA(SqlKind.OR)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            int size = operands.size();
            int i = 0;
            for (; i < size; i++) {
                RexCall f = (RexCall) operands.get(i);
                if (f.isA(SqlKind.EQUALS)) {
                    RexCall call = (RexCall) f;
                    RexNode left = (RexNode) call.getOperands().get(0);
                    if (left.isA(SqlKind.CAST)) {
                        left = (RexNode) ((RexCall) left).operands.get(0);
                    }
                    RexNode right = (RexNode) call.getOperands().get(1);
                    if (left instanceof RexInputRef && right instanceof RexLiteral) {
                        int index = ((RexInputRef) left).getIndex();
                        String value = ((RexLiteral) right).getValue2().toString();
                        dataMappingRule.assignment(true, index, value);
                        continue;
                    }
                }
                break;
            }
            return i == size;
        }
        return addFilter(filter);
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
        String filterText = "";
        if (!filters.isEmpty()) {
            filters.removeIf((filter) -> addOrFilter(filter));
            filterText = dataMappingRule.getFilterExpr();
        }
        List<BackEndTableInfo> backStoreList = this.backStoreList;
        int[] calculate = dataMappingRule.calculate();

        if (filters.isEmpty()){
            if (calculate.length == 0) {
                backStoreList = this.backStoreList;
            }
            if (calculate.length == 1) {
                backStoreList = Collections.singletonList(this.backStoreList.get(calculate[0]));
            }

            if (calculate.length > 1) {
                backStoreList = new ArrayList<>(calculate.length);
                int size = this.backStoreList.size();
                for (int i1 : calculate) {
                    if (i1 >= size) {
                        backStoreList = this.backStoreList;
                        break;
                    } else {
                        backStoreList.add(this.backStoreList.get(i1));
                    }
                }
            }
        }
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        if (projects == null) {

            return new MyCatResultSetEnumerable(cancelFlag,backStoreList, "*", filterText);
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
}
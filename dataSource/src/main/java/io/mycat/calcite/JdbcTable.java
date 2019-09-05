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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private boolean addFilter(RexNode filter, Object[] filterValues) {
        if (filter.isA(SqlKind.AND)) {
            ((RexCall) filter).getOperands().forEach((subFilter) -> {
                this.addFilter(subFilter, filterValues);
            });
        } else if (filter.isA(SqlKind.EQUALS)) {
            RexCall call = (RexCall) filter;
            RexNode left = (RexNode) call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = (RexNode) ((RexCall) left).operands.get(0);
            }

            RexNode right = (RexNode) call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                if (filterValues[index] == null) {
                    filterValues[index] = ((RexLiteral) right).getValue2().toString();
                    return true;
                }
            }
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                if (filterValues[index] == null) {
                    filterValues[index] = ((RexLiteral) right).getValue2().toString();
                    return true;
                }
            }
        }
        return false;
    }

    private boolean addOrFilter(RexNode filter, Object[] filterValues) {
        if (filter.isA(SqlKind.OR)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            int size = operands.size();
            int i = 0;
            for (; i < size; i++) {
                if (!addFilter(operands.get(i), filterValues)) {
                    break;
                }
            }
           return i == size;
        }
        return false;
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
        final String[] filterValues = new String[this.rowSignature.getColumnCount()];
        if (filters.isEmpty()) {

        } else {
            StringBuilder sb = new StringBuilder("true");

            filters.removeIf((filter) -> this.addFilter(filter, filterValues));
            for (int i = 0; i < filterValues.length; i++) {
                String filterValue = filterValues[i];
                if (filterValue == null) {
                    continue;
                }
                sb.append(" and ").append(rowSignature.getRowOrder().get(i)).append("=").append(filterValue);
            }
            ////////////////////////////////////////////////////////////////
//            filters.removeIf(filter -> addOrFilter(filter,filterValues));
//            if (!filters.isEmpty()) {
//                throw new UnsupportedOperationException();
//            }

//            for (RexNode filter : filters) {
//                if (filter instanceof RexCall) {
//                    RexCall call = (RexCall) filter;
//                    if (call.getOperands().size() == 2) {
//                        RexNode left = call.getOperands().get(0);
//                        RexNode right = call.getOperands().get(1);
//
//                        RexInputRef input = null;
//                        RexLiteral literal = null;
//
//                        if (left instanceof RexInputRef && right instanceof RexLiteral) {
//                            input = (RexInputRef) left;
//                            literal = (RexLiteral) right;
//                        } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
//                            input = (RexInputRef) right;
//                            literal = (RexLiteral) left;
//                        } else {
//                            continue;
//                        }
//                        sb.append(rowSignature.getRowOrder().get(input.getIndex())).
//                                append(call.op)
//                                .append(literal.getValue2().toString());
//
//                    }
//                }
//            }
            filterText = sb.toString();
        }
        List<BackEndTableInfo> backStoreList = this.backStoreList;
        if (filters.isEmpty()){
            for (int i = 0; i <filterValues.length; i++) {
                String filterValue = filterValues[i];
                if (filterValue!=null){
                    dataMappingRule.assignment(i,filterValue);
                    int[] calculate = dataMappingRule.calculate();
                    if (calculate.length==1){
                        backStoreList = Collections.singletonList(this.backStoreList.get(calculate[0]));
                        break;
                    }
                }
            }
        }

        if (projects == null) {
            return new MyCatResultSetEnumerable(backStoreList, "*", filterText);
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
            return new MyCatResultSetEnumerable(backStoreList, projectText.toString(), filterText);
        }
    }
}
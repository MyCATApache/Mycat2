package io.mycat.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
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

import java.util.List;

public class JdbcTable implements TranslatableTable, FilterableTable {
    private final String schemaName;
    private final String tableName;
    private final RelProtoDataType relProtoDataType;
    private final RowSignature rowSignature;
    private final List<BackEndTableInfo> backStoreList;

    public JdbcTable(String schemaName, String tableName, List<BackEndTableInfo> value, RelProtoDataType relProtoDataType, RowSignature rowSignature) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.backStoreList = value;
        this.relProtoDataType = relProtoDataType;
        this.rowSignature = rowSignature;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        final String[] filterValues = new String[this.rowSignature.getColumnCount()];
        filters.removeIf((filter) -> this.addFilter(filter, filterValues));
        final JavaTypeFactory typeFactory = root.getTypeFactory();
        for (RexNode filter : filters) {
            if (filter.isA(SqlKind.EQUALS)) {

            }
        }

        return null;
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
        }

        return false;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        return LogicalTableScan.create(toRelContext.getCluster(), relOptTable);
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
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
}
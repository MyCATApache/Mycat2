package io.mycat.calcite;

import io.mycat.api.collector.RowBaseIterator;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;
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
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JdbcTable implements TranslatableTable, FilterableTable {
    private final String schemaName;
    private final String tableName;
    private final RelProtoDataType relProtoDataType;
    private final RowSignature rowSignature;
    private final List<BackEndTableInfo> backStoreList;
    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcTable.class);

    public JdbcTable(String schemaName, String tableName, List<BackEndTableInfo> value, RelProtoDataType relProtoDataType, RowSignature rowSignature) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.backStoreList = value;
        this.relProtoDataType = relProtoDataType;
        this.rowSignature = rowSignature;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        List<Pair<ColumnMetaData.Rep, Integer>> list = CalciteConvertors.fieldClasses(relProtoDataType, root.getTypeFactory());
        int columnCount = list.size();
        Function1<RowBaseIterator, Object[]> transfor = a0 -> {
            final Object[] values = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                values[i] = a0.getObject(columnCount);
            }
            return values;
        };
        String filterText = "";
        if (filters.isEmpty()) {

        } else {
            final String[] filterValues = new String[this.rowSignature.getColumnCount()];
            filters.removeIf((filter) -> this.addFilter(filter, filterValues));
            for (RexNode filter : filters) {
                if (filter instanceof RexCall) {
                    RexCall call = (RexCall) filter;
                    if (call.isA(SqlKind.EQUALS) && call.getOperands().size() == 2) {
                        RexNode left = call.getOperands().get(0);
                        RexNode right = call.getOperands().get(1);


                        RexInputRef input = null;
                        RexLiteral literal = null;

                        if (left instanceof RexInputRef && right instanceof RexLiteral) {
                            input = (RexInputRef) left;
                            literal = (RexLiteral) right;
                        } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
                            input = (RexInputRef) right;
                            literal = (RexLiteral) left;
                        } else {
                            continue;
                        }

                        StringBuilder sb = new StringBuilder();
                        sb.append(rowSignature.getRowOrder().get(input.getIndex())).
                                append("=")
                                .append(literal.getValue2().toString());
                        filterText = sb.toString();
                    }

                }
            }
        }
        return new MyCatResultSetEnumerable(backStoreList, filterText);
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
}
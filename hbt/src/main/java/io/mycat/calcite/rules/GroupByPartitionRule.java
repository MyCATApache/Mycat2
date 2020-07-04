package io.mycat.calcite.rules;

import com.google.common.base.Predicate;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.metadata.ShardingTable;
import io.mycat.TableHandler;
import io.mycat.SimpleColumnInfo;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.EnumMap;
import java.util.List;

public class GroupByPartitionRule extends RelOptRule {
    public static final EnumMap<SqlKind, Boolean> SUPPORTED_AGGREGATES = new EnumMap<>(SqlKind.class);

    static {
        SUPPORTED_AGGREGATES.put(SqlKind.MIN, true);
        SUPPORTED_AGGREGATES.put(SqlKind.MAX, true);
        SUPPORTED_AGGREGATES.put(SqlKind.COUNT, true);
        SUPPORTED_AGGREGATES.put(SqlKind.SUM, true);
        SUPPORTED_AGGREGATES.put(SqlKind.SUM0, true);
    }

    public GroupByPartitionRule() {
        super(operandJ(Aggregate.class, null, GroupByPartitionRule::test,
                operandJ(Union.class, null, r -> !r.isDistinct(),
                        operandJ(TableScan.class, null, (Predicate<TableScan>) input -> {
                            if (input != null) {
                                RelOptTable table = input.getTable();
                                if (table != null) {
                                    MycatPhysicalTable table1 = table.unwrap(MycatPhysicalTable.class);
                                    MycatLogicTable logicTable = table1.getLogicTable();
                                    TableHandler tableHandler = logicTable.getTable();
                                    if (tableHandler instanceof ShardingTable) {
                                        ShardingTable handler = (ShardingTable) tableHandler;
                                        return logicTable.getPhysicalTables().size() > 1;
                                    }
                                    return false;
                                }
                            }
                            return false;
                        }, none()))));
    }

    private static boolean test(Aggregate r) {
        return r.getGroupCount() == 1 && r.getAggCallList().size() == 1 &&
                SUPPORTED_AGGREGATES.containsKey(r.getAggCallList().get(0).getAggregation().kind);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        final Union union = call.rel(1);
        final TableScan table = call.rel(2);
        if (!test(aggregate)) {
            return;
        }

        ImmutableBitSet groupSet = aggregate.getGroupSet();
        Integer integer = groupSet.asList().get(0);
        MycatPhysicalTable table1 = table.getTable().unwrap(MycatPhysicalTable.class);

        MycatLogicTable logicTable = table1.getLogicTable();
        TableHandler tableHandler = logicTable.getTable();
        if (tableHandler instanceof ShardingTable) {
            ShardingTable handler = (ShardingTable) tableHandler;
            List<SimpleColumnInfo> columns = handler.getColumns();
            if (integer < columns.size()) {
                if (handler.getNatureTableColumnInfo().getColumnInfo().equals(columns.get(integer))) {
                    RelBuilder builder = call.builder();
                    List<RelNode> inputs = union.getInputs();


                }
            }
        }

    }
}
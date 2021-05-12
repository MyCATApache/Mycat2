package io.mycat.calcite.localrel;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

public class LocalTableScan extends TableScan implements LocalRel{
    public LocalTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.9);
    }
    public static final RelFactories.TableScanFactory TABLE_SCAN_FACTORY =
            (toRelContext, table) -> {
               return new LocalTableScan(toRelContext.getCluster(),toRelContext.getCluster().traitSet(), toRelContext.getTableHints(),table);
            };
}

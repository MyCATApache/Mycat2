package io.mycat.calcite.localrel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

public class LocalWindow extends Window implements LocalRel {

    public LocalWindow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<RexLiteral> constants, RelDataType rowType, List<Group> groups) {
        super(cluster, traitSet.replace(LocalConvention.INSTANCE), input, constants, rowType, groups);
    }

    public LocalWindow(RelInput relInput) {
        this(relInput.getCluster(), relInput.getTraitSet(), relInput.getInput(), (List) relInput.getExpressionList("constants"), relInput.getRowType("rowType"),
                (List) relInput.getWindowGroupList());
    }

    public static LocalWindow create(LogicalWindow logicalWindow) {
        return new LocalWindow(logicalWindow.getCluster(),
                logicalWindow.getTraitSet(),
                logicalWindow.getInput(), logicalWindow.getConstants(), logicalWindow.getRowType(), logicalWindow.groups);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LocalWindow(getCluster(), traitSet, inputs.get(0), getConstants(), getRowType(), groups);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.9);
    }
}

package io.mycat.calcite.localrel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class LocalSort extends Sort implements LocalRel{
    public LocalSort(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traits.replace(LocalConvention.INSTANCE), child, collation,offset,fetch);
    }

    public LocalSort(RelInput input) {
        this(input.getCluster(), input.getTraitSet().plus(input.getCollation()),
                input.getInput(),
                RelCollationTraitDef.INSTANCE.canonize(input.getCollation()),
                input.getExpression("offset"), input.getExpression("fetch"));
    }

    public static LocalSort create(Sort logicalSort, RelNode input) {
        return new LocalSort(logicalSort.getCluster(), logicalSort.getTraitSet(), input, logicalSort.getCollation(),logicalSort.offset,logicalSort.fetch);
    }

    @Override
    public LocalSort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new LocalSort(getCluster(),traitSet,newInput,collation,offset,fetch);
    }
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.9);
    }
    public static final RelFactories.SortFactory SORT_FACTORY =
            (input, collation, offset, fetch) -> {
                throw new UnsupportedOperationException("LocalSort");
            };

}

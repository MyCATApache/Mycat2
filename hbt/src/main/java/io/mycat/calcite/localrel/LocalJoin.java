package io.mycat.calcite.localrel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.calcite.MycatRelDataTypeUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LocalJoin extends Join implements LocalRel {
    protected LocalJoin(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet.replace(LocalConvention.INSTANCE), hints, left, right, condition, variablesSet, joinType);
    }
    public LocalJoin(RelInput input) {
        this(input.getCluster(), input.getTraitSet(),
                ImmutableList.of(),
                input.getInputs().get(0), input.getInputs().get(1),
                input.getExpression("condition"), ImmutableSet.of(),
                input.getEnum("joinType", JoinRelType.class));
    }
    public static LocalJoin create(Join join, RelNode left, RelNode right) {
        return new LocalJoin(join.getCluster(), join.getTraitSet(), join.getHints(), left,right,join.getCondition(),join.getVariablesSet(),join.getJoinType());
    }

    @Override
    public LocalJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new LocalJoin(getCluster(),traitSet,getHints(),left,right,conditionExpr,getVariablesSet(),joinType);
    }
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.9);
    }

    static final RelFactories.JoinFactory JOIN_FACTORY =
            (left, right, hints, condition, variablesSet, joinType, semiJoinDone) -> {
                final RelOptCluster cluster = left.getCluster();
                final RelTraitSet traitSet = cluster.traitSetOf(
                        requireNonNull(left.getConvention(), "left.getConvention()"));
                return new LocalJoin(cluster, traitSet,hints, left, right, condition,
                        variablesSet, joinType);
            };

    static final RelFactories.CorrelateFactory CORRELATE_FACTORY =
            (left, right, correlationId, requiredColumns, joinType) -> {
                throw new UnsupportedOperationException("LocalCorrelate");
            };
    @Override
    public MycatRelDataType getMycatRelDataType() {
        LocalRel left = (LocalRel) getLeft();
        LocalRel right =  (LocalRel)getRight();
        return left.getMycatRelDataType().join(right.getMycatRelDataType());
    }
}

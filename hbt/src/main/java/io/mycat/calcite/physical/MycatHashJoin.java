package io.mycat.calcite.physical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelNodes;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Set;

public class MycatHashJoin extends Join implements MycatRel {
    protected MycatHashJoin(RelOptCluster cluster,
                            RelTraitSet traitSet,
                            RelNode left,
                            RelNode right,
                            RexNode condition,
                            Set<CorrelationId> variablesSet,
                            JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    /**
     * Creates an MycatHashJoin.
     */
    public static MycatHashJoin create(
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType) {
        final RelOptCluster cluster = left.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        traitSet =
                traitSet.replace(MycatConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.enumerableHashJoin(mq, left, right, joinType));
        return new MycatHashJoin(cluster, traitSet, left, right, condition,
                ImmutableSet.of(), joinType);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatHashJoin").into();
        for (RelNode input : getInputs()) {
            MycatRel mycatRel = (MycatRel) input;
            mycatRel.explain(writer);
        }
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public MycatHashJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new MycatHashJoin(getCluster(), traitSet,left, right, conditionExpr, getVariablesSet(), joinType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);

        // Joins can be flipped, and for many algorithms, both versions are viable
        // and have the same cost. To make the results stable between versions of
        // the planner, make one of the versions slightly more expensive.
        switch (joinType) {
            case SEMI:
            case ANTI:
                // SEMI and ANTI join cannot be flipped
                break;
            case RIGHT:
                rowCount = RelMdUtil.addEpsilon(rowCount);
                break;
            default:
                if (RelNodes.COMPARATOR.compare(left, right) > 0) {
                    rowCount = RelMdUtil.addEpsilon(rowCount);
                }
        }

        // Cheaper if the smaller number of rows is coming from the LHS.
        // Model this by adding L log L to the cost.
        final double rightRowCount = right.estimateRowCount(mq);
        final double leftRowCount = left.estimateRowCount(mq);
        if (Double.isInfinite(leftRowCount)) {
            rowCount = leftRowCount;
        } else {
            rowCount += Util.nLogN(leftRowCount);
        }
        if (Double.isInfinite(rightRowCount)) {
            rowCount = rightRowCount;
        } else {
            rowCount += rightRowCount;
        }
        RelOptCost relOptCost;
        if (isSemiJoin()) {
            relOptCost = planner.getCostFactory().makeCost(rowCount, 0, 0).multiplyBy(.01d);
        } else {
            relOptCost = planner.getCostFactory().makeCost(rowCount, 0, 0);
        }
        return relOptCost;
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        EnumerableHashJoin enumerableHashJoin = EnumerableHashJoin.create(left, right, condition, variablesSet, joinType);
        Result result = enumerableHashJoin.implement(implementor, pref);
        return result;
    }
}
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
                            List<RelHint> hints,
                            RelNode left,
                            RelNode right,
                            RexNode condition,
                            Set<CorrelationId> variablesSet,
                            JoinRelType joinType) {
        super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    }

    /**
     * Creates an MycatHashJoin.
     */
    public static MycatHashJoin create(
            ImmutableList<RelHint> hints,
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
        return new MycatHashJoin(cluster, traitSet, hints, left, right, condition,
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
        return new MycatHashJoin(getCluster(), traitSet, ImmutableList.of(), left, right, conditionExpr, getVariablesSet(), joinType);
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
        return implementHashJoin(implementor,pref);
    }

    private  Result implementHashJoin(MycatEnumerableRelImplementor implementor, Prefer pref) {
        BlockBuilder builder = new BlockBuilder();
        final Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) left, pref);
        Expression leftExpression =
                builder.append(
                        "left", leftResult.block);
        final Result rightResult =
                implementor.visitChild(this, 1, (EnumerableRel) right, pref);
        Expression rightExpression =
                builder.append(
                        "right", rightResult.block);
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(), getRowType(), pref.preferArray());
        final PhysType keyPhysType =
                leftResult.physType.project(
                        joinInfo.leftKeys, JavaRowFormat.LIST);
        Expression predicate = Expressions.constant(null);
        if (!joinInfo.nonEquiConditions.isEmpty()) {
            RexNode nonEquiCondition = RexUtil.composeConjunction(
                    getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
            if (nonEquiCondition != null) {
                predicate = EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
                        left, right, leftResult.physType, rightResult.physType, nonEquiCondition);
            }
        }
        return implementor.result(
                physType,
                builder.append(
                        Expressions.call(
                                leftExpression,
                                BuiltInMethod.HASH_JOIN.method,
                                Expressions.list(
                                        rightExpression,
                                        leftResult.physType.generateAccessor(joinInfo.leftKeys),
                                        rightResult.physType.generateAccessor(joinInfo.rightKeys),
                                        EnumUtils.joinSelector(joinType,
                                                physType,
                                                ImmutableList.of(
                                                        leftResult.physType, rightResult.physType)))
                                        .append(
                                                Util.first(keyPhysType.comparer(),
                                                        Expressions.constant(null)))
                                        .append(
                                                Expressions.constant(joinType.generatesNullsOnLeft()))
                                        .append(
                                                Expressions.constant(
                                                        joinType.generatesNullsOnRight()))
                                        .append(predicate)))
                        .toBlock());
    }
}
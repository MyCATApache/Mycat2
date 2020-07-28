package io.mycat.hbt4.executor;

import io.mycat.hbt3.MycatLookUpView;
import io.mycat.hbt3.View;
import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * incomplete
 */
public class MycatBatchNestedLoopJoinRule extends RelOptRule {
    public static final MycatBatchNestedLoopJoinRule INSTANCE = new MycatBatchNestedLoopJoinRule();

    int batchSize = DEFAULT_BATCH_SIZE;
    private static final int DEFAULT_BATCH_SIZE = 1000;

    public MycatBatchNestedLoopJoinRule() {
        super(operandJ(Join.class, null, join -> {
                    JoinRelType joinType = join.getJoinType();
                    boolean b = joinType == JoinRelType.INNER
                            || joinType == JoinRelType.LEFT
                            || joinType == JoinRelType.ANTI
                            || joinType == JoinRelType.SEMI;
                    if (!b) {
                        return false;
                    }
                    JoinInfo joinInfo = join.analyzeCondition();
                    return joinInfo.isEqui() && !joinInfo.leftSet().isEmpty();
                },
                operand(MycatRel.class, any()),
                operand(View.class, any())), "MycatBatchNestedLoopJoinRule");
    }


    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        JoinRelType joinType = join.getJoinType();
        return joinType == JoinRelType.INNER
                || joinType == JoinRelType.LEFT
                || joinType == JoinRelType.ANTI
                || joinType == JoinRelType.SEMI;
    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final View lookupView = call.rel(2);
        final int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        final RelOptCluster cluster = join.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RelBuilder relBuilder = call.builder();

        final Set<CorrelationId> correlationIds = new HashSet<>();
        JoinInfo joinInfo = join.analyzeCondition();
        RexNode equiCondition = joinInfo.getEquiCondition(call.rel(1), call.rel(2), rexBuilder);
        System.out.println(joinInfo);

        final ArrayList<RexNode> corrVar = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            CorrelationId correlationId = cluster.createCorrel();
            correlationIds.add(correlationId);
            corrVar.add(
                    rexBuilder.makeCorrel(join.getLeft().getRowType(),
                            correlationId));
        }
        final ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();

        // Generate first condition
        RexNode condition = join.getCondition().accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef input) {
                int field = input.getIndex();
                if (field >= leftFieldCount) {
                    return rexBuilder.makeInputRef(input.getType(),
                            input.getIndex() - leftFieldCount);
                }
                requiredColumns.set(field);
                return  rexBuilder.makeFieldAccess(corrVar.get(0), field);
            }
        });
        List<RexNode> conditionList = new ArrayList<>();
        conditionList.add(condition);

        // Add batchSize-1 other conditions
        for (int i = 1; i < batchSize; i++) {
            final int corrIndex = i;
            final RexNode condition2 = condition.accept(new RexShuttle() {
                @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                    return corrVar.get(corrIndex);
                }
            });
            conditionList.add(condition2);
        }
        // Push a filter with batchSize disjunctions
        relBuilder.push(lookupView.getRelNode()).filter(relBuilder.or(conditionList));
        RelNode right = MycatLookUpView.create(relBuilder.build());

        JoinRelType joinType = join.getJoinType();
        call.transformTo(
                MycatBatchNestedLoopJoin.create(
                        convert(join.getLeft(), join.getLeft().getTraitSet()
                                .replace(MycatConvention.INSTANCE)),
                        convert(right, right.getTraitSet()
                                .replace(MycatConvention.INSTANCE)),
                        join.getCondition(),
                        requiredColumns.build(),
                        correlationIds,
                        joinType));
    }
}

package io.mycat.calcite.rules;

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatSQLTableLookup;
import io.mycat.calcite.physical.MycatTableLookupValues;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

public class MycatTableLookupBottomRule extends RelRule<MycatTableLookupBottomRule.Config> {

    public MycatTableLookupBottomRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelOptCluster cluster = join.getCluster();
        RelNode left = call.rel(1);
        RelNode right = call.rel(2);
        JoinInfo joinInfo = join.analyzeCondition();
        if (!joinInfo.isEqui()) {
            return;
        }
        if (!(right instanceof MycatView)) {
            return;
        }
        MycatView mycatView = (MycatView) right;
        if (mycatView.isMergeSort() || mycatView.isMergeAgg()) {
            return;
        }
        JoinRelType joinType = join.getJoinType();
        switch (joinType) {
            case INNER:
            case LEFT:
            case RIGHT:
                break;
            default:
                return;
        }
        RelBuilder relBuilder = call.builder();
        RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        List<RexNode> leftExprs = new ArrayList<>();
        List<CorrelationId> correlationIds = new ArrayList<>();
        {
            for (Integer leftKey : join.analyzeCondition().leftSet()) {
                CorrelationId correl = cluster.createCorrel();
                correlationIds.add(correl);
                RelDataType type = left.getRowType().getFieldList().get(leftKey).getType();
                RexNode rexNode = rexBuilder.makeCorrel(typeFactory.createUnknownType(), correl);
                leftExprs.add(rexBuilder.makeCast(type, rexNode));
            }
        }
        switch (mycatView.getDistribution().type()) {
            case PHY:
            case BROADCAST:
                RelBuilder builder = call.builder();
                builder.push(MycatTableLookupValues.create(cluster, left.getRowType(), leftExprs, left.getTraitSet()))
                        .push(mycatView.getRelNode())
                        .join(joinType, join.getCondition());
                MycatView view = mycatView.changeTo(relBuilder.build());
                call.transformTo(new MycatSQLTableLookup(cluster, join.getTraitSet(), left, view, joinType, join.getCondition(),correlationIds, MycatSQLTableLookup.Type.NONE));
                break;
            default:
        }
    }

    public interface Config extends RelRule.Config {
        MycatTableLookupBottomRule.Config DEFAULT = EMPTY
                .as(MycatTableLookupBottomRule.Config.class)
                .withOperandFor(b0 ->
                        b0.operand(Join.class).inputs(b1 -> b1.operand(MycatSQLTableLookup.class).anyInputs(), b1 -> b1.operand(MycatView.class).noInputs()))
                .withDescription("MycatTableLookupBottomRule")
                .as(MycatTableLookupBottomRule.Config.class);

        @Override
        default MycatTableLookupBottomRule toRule() {
            return new MycatTableLookupBottomRule(this);
        }

        default MycatTableLookupBottomRule.Config withOperandFor(OperandTransform transform) {
            return withOperandSupplier(transform)
                    .as(MycatTableLookupBottomRule.Config.class);
        }
    }
}

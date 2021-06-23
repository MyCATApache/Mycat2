package io.mycat.calcite.rules;

import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatSQLTableLookup;
import io.mycat.calcite.physical.MycatTableLookupValues;
import io.mycat.calcite.rewriter.RBORules;
import io.mycat.calcite.rewriter.SQLRBORewriter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilder;

import java.util.Optional;

public class MycatTableLookupCombineRule extends RelRule<MycatTableLookupCombineRule.Config> {

    public MycatTableLookupCombineRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelOptCluster cluster = join.getCluster();
        MycatSQLTableLookup left = call.rel(1);
        if (left.getType() != MycatSQLTableLookup.Type.NONE) {
            return;
        }
        RelNode right = call.rel(2);
        JoinInfo joinInfo = join.analyzeCondition();
        if (!(right instanceof MycatView)) {
            return;
        }
        MycatView inneRightMycatView = (MycatView) left.getRight();
        MycatView outerRightmycatView = (MycatView) right;
        if (outerRightmycatView.isMergeSort() || outerRightmycatView.isMergeAgg()) {
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
        switch (outerRightmycatView.getDistribution().type()) {
            case PHY:
            case BROADCAST: {
                RelBuilder builder = call.builder();
                inneRightMycatView.getDistribution().join(outerRightmycatView.getDistribution())
                        .ifPresent(c -> {
                            RelBuilder relBuilder = builder
                                    .push(inneRightMycatView.getRelNode())
                                    .push(outerRightmycatView.getRelNode())
                                    .join(joinType, join.getCondition());
                            RelNode relNode = relBuilder.build();
                            MycatView view = MycatView.ofCondition(relNode, c, null);
                            call.transformTo(left.changeTo(left.getInput(0),view));
                        });
                break;
            }
            case SHARDING: {
                SQLRBORewriter
                        .bottomJoin(inneRightMycatView, outerRightmycatView, join)
                        .ifPresent(relNode1 -> {
                    call.transformTo(left.changeTo(left.getInput(0),(MycatView) relNode1));
                });
                break;
            }
            default:
        }
    }

    public interface Config extends RelRule.Config {
        MycatTableLookupCombineRule.Config DEFAULT = EMPTY
                .as(MycatTableLookupCombineRule.Config.class)
                .withOperandFor(b0 ->
                        b0.operand(Join.class).inputs(b1 -> b1.operand(MycatSQLTableLookup.class)
                                        .predicate(i -> i.getType() == MycatSQLTableLookup.Type.NONE).anyInputs(),
                                b1 -> b1.operand(MycatView.class).noInputs()))
                .withDescription("MycatTableLookupCombineRule")
                .as(MycatTableLookupCombineRule.Config.class);

        @Override
        default MycatTableLookupCombineRule toRule() {
            return new MycatTableLookupCombineRule(this);
        }

        default MycatTableLookupCombineRule.Config withOperandFor(OperandTransform transform) {
            return withOperandSupplier(transform)
                    .as(MycatTableLookupCombineRule.Config.class);
        }
    }
}

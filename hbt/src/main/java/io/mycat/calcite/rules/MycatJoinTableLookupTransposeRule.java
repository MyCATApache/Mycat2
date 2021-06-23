package io.mycat.calcite.rules;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatSQLTableLookup;
import io.mycat.calcite.physical.MycatTableLookupValues;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilder;

public class MycatJoinTableLookupTransposeRule extends RelRule<MycatJoinTableLookupTransposeRule.Config> {

    public MycatJoinTableLookupTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelOptCluster cluster = join.getCluster();
        RelNode outerLeft = call.rel(1);
        RelNode outerRight = call.rel(2);

        if (outerLeft instanceof MycatSQLTableLookup){
            MycatSQLTableLookup mycatSQLTableLookup = (MycatSQLTableLookup) outerLeft;
            RelNode bottomInput = mycatSQLTableLookup.getInput();
            MycatView indexRightView = (MycatView)mycatSQLTableLookup.getRight();
            RelNode newInputJoin  = join.copy(bottomInput.getTraitSet(),ImmutableList.of(bottomInput,outerRight));
            call.transformTo(mycatSQLTableLookup.copy(join.getTraitSet(), ImmutableList.of(newInputJoin,indexRightView)));
        }
        if (outerRight instanceof MycatSQLTableLookup){
            MycatSQLTableLookup mycatSQLTableLookup = (MycatSQLTableLookup) outerRight;
            MycatView indexRightView = (MycatView)mycatSQLTableLookup.getRight();
            RelNode newInputJoin  = join.copy(outerLeft.getTraitSet(),ImmutableList.of(mycatSQLTableLookup.getInput(),indexRightView));
            call.transformTo(mycatSQLTableLookup.copy(join.getTraitSet(), ImmutableList.of(newInputJoin,indexRightView)));
        }
    }

    public interface Config extends RelRule.Config {
        MycatJoinTableLookupTransposeRule.Config DEFAULT_LEFT = EMPTY
                .as(MycatJoinTableLookupTransposeRule.Config.class)
                .withOperandFor(b0 ->
                        b0.operand(Join.class).inputs(b1 -> b1.operand(MycatSQLTableLookup.class).anyInputs(), b1 -> b1.operand(MycatView.class).noInputs()))
                .withDescription("MycatJoinTableLookupLeftTransposeRule")
                .as(MycatJoinTableLookupTransposeRule.Config.class);
        MycatJoinTableLookupTransposeRule.Config DEFAULT_RIGHT = EMPTY
                .as(MycatJoinTableLookupTransposeRule.Config.class)
                .withOperandFor(b0 ->
                        b0.operand(Join.class).inputs(b1 -> b1.operand(MycatView.class).noInputs(), b1 -> b1.operand(MycatSQLTableLookup.class).anyInputs()))
                .withDescription("MycatJoinTableLookupRightTransposeRule")
                .as(MycatJoinTableLookupTransposeRule.Config.class);

        @Override
        default MycatJoinTableLookupTransposeRule toRule() {
            return new MycatJoinTableLookupTransposeRule(this);
        }

        default MycatJoinTableLookupTransposeRule.Config withOperandFor(OperandTransform transform) {
            return withOperandSupplier(transform)
                    .as(MycatJoinTableLookupTransposeRule.Config.class);
        }
    }
}

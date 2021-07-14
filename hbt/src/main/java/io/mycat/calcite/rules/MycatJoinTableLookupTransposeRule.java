//package io.mycat.calcite.rules;
//
//import com.google.common.collect.ImmutableList;
//import io.mycat.calcite.MycatCalciteSupport;
//import io.mycat.calcite.MycatRel;
//import io.mycat.calcite.MycatRules;
//import io.mycat.calcite.logical.MycatView;
//import io.mycat.calcite.physical.MycatSQLTableLookup;
//import io.mycat.calcite.physical.MycatTableLookupValues;
//import org.apache.calcite.plan.RelOptCluster;
//import org.apache.calcite.plan.RelOptRuleCall;
//import org.apache.calcite.plan.RelOptUtil;
//import org.apache.calcite.plan.RelRule;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.core.Join;
//import org.apache.calcite.rel.core.JoinInfo;
//import org.apache.calcite.rel.core.JoinRelType;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.tools.RelBuilder;
//
//import java.util.Optional;
//
//public class MycatJoinTableLookupTransposeRule extends RelRule<MycatJoinTableLookupTransposeRule.Config> {
//
//    public static final MycatJoinTableLookupTransposeRule LEFT_INSTANCE = Config.DEFAULT_LEFT.toRule();
//
//    public static final MycatJoinTableLookupTransposeRule RIGHT_INSTANCE = Config.DEFAULT_RIGHT.toRule();
//
//    public MycatJoinTableLookupTransposeRule(Config config) {
//        super(config);
//    }
//
//    @Override
//    public void onMatch(RelOptRuleCall call) {
//        Join join = call.rel(0);
//        RelDataType originalRowType = join.getRowType();
//        RelOptCluster cluster = join.getCluster();
//        RelNode outerLeft = call.rel(1);
//        RelNode outerRight = call.rel(2);
//
//        if (outerLeft instanceof MycatSQLTableLookup) {
//            MycatSQLTableLookup mycatSQLTableLookup = (MycatSQLTableLookup) outerLeft;
//            if (mycatSQLTableLookup.getType() == MycatSQLTableLookup.Type.BACK) {
//                RelNode bottomInput = mycatSQLTableLookup.getInput();
//                MycatView indexRightView = (MycatView) mycatSQLTableLookup.getRight();
//                RelNode newInputJoin = join.copy(bottomInput.getTraitSet(), ImmutableList.of(bottomInput, outerRight));
//                MycatSQLTableLookup newMycatSQLTableLookup = new MycatSQLTableLookup(join.getCluster(),
//                        join.getTraitSet(),
//                        newInputJoin,
//                        indexRightView,
//                        mycatSQLTableLookup.getJoinType(),
//                        mycatSQLTableLookup.getCondition(),
//                        mycatSQLTableLookup.getCorrelationIds(),
//                        mycatSQLTableLookup.getType());
//                call.transformTo(fixProject(originalRowType, newMycatSQLTableLookup));
//                return;
//            }
//
//        }
//        if (outerRight instanceof MycatSQLTableLookup) {
//            MycatSQLTableLookup mycatSQLTableLookup = (MycatSQLTableLookup) outerRight;
//            MycatView indexRightView = (MycatView) mycatSQLTableLookup.getRight();
//            RelNode newInputJoin = join.copy(outerLeft.getTraitSet(), ImmutableList.of(mycatSQLTableLookup.getInput(), indexRightView));
//            if (mycatSQLTableLookup.getType() == MycatSQLTableLookup.Type.BACK) {
//                MycatSQLTableLookup newMycatSQLTableLookup = new MycatSQLTableLookup(join.getCluster(),
//                        join.getTraitSet(),
//                        newInputJoin,
//                        indexRightView,
//                        mycatSQLTableLookup.getJoinType(),
//                        mycatSQLTableLookup.getCondition(),
//                        mycatSQLTableLookup.getCorrelationIds(),
//                        mycatSQLTableLookup.getType());
//                call.transformTo(fixProject(originalRowType, newMycatSQLTableLookup));
//                return;
//            }
//        }
//    }
//
//    private Optional<RelNode> fixProject(RelDataType originalRowType, MycatSQLTableLookup newMycatSQLTableLookup) {
//        RelNode resNode;
//        if (!RelOptUtil.areRowTypesEqual(originalRowType, newMycatSQLTableLookup.getRowType(), false)) {
//            resNode = MycatView.createMycatProject(newMycatSQLTableLookup, originalRowType.getFieldNames());
//        } else {
//            resNode = newMycatSQLTableLookup;
//        }
//        if (originalRowType.isNullable()==resNode.getRowType().isNullable()){
//            return Optional.of(resNode);
//        }
//        if (originalRowType.isNullable()&&!resNode.getRowType().isNullable()){
//            MycatCalciteSupport.TypeFactory.createTypeWithNullability()
//        }
//
//        return resNode;
//    }
//
//    public interface Config extends RelRule.Config {
//        MycatJoinTableLookupTransposeRule.Config DEFAULT_LEFT = EMPTY
//                .as(MycatJoinTableLookupTransposeRule.Config.class)
//                .withOperandFor(b0 ->
//                        b0.operand(Join.class).inputs(b1 -> b1.operand(MycatSQLTableLookup.class).anyInputs(), b1 -> b1.operand(MycatView.class).noInputs()))
//                .withDescription("MycatJoinTableLookupLeftTransposeRule")
//                .as(MycatJoinTableLookupTransposeRule.Config.class);
//        MycatJoinTableLookupTransposeRule.Config DEFAULT_RIGHT = EMPTY
//                .as(MycatJoinTableLookupTransposeRule.Config.class)
//                .withOperandFor(b0 ->
//                        b0.operand(Join.class).inputs(b1 -> b1.operand(MycatView.class).noInputs(), b1 -> b1.operand(MycatSQLTableLookup.class).anyInputs()))
//                .withDescription("MycatJoinTableLookupRightTransposeRule")
//                .as(MycatJoinTableLookupTransposeRule.Config.class);
//
//        @Override
//        default MycatJoinTableLookupTransposeRule toRule() {
//            return new MycatJoinTableLookupTransposeRule(this);
//        }
//
//        default MycatJoinTableLookupTransposeRule.Config withOperandFor(OperandTransform transform) {
//            return withOperandSupplier(transform)
//                    .as(MycatJoinTableLookupTransposeRule.Config.class);
//        }
//    }
//}

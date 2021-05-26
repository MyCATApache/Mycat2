//package io.mycat.calcite.rules;
//
//import io.mycat.calcite.logical.MycatView;
//import io.mycat.calcite.rewriter.Distribution;
//import io.mycat.calcite.table.MycatLogicTable;
//import org.apache.calcite.plan.RelOptRuleCall;
//import org.apache.calcite.plan.RelRule;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.core.Filter;
//import org.apache.calcite.rel.core.Join;
//import org.apache.calcite.rel.core.JoinRelType;
//import org.apache.calcite.rel.core.TableScan;
//import org.apache.calcite.rel.logical.LogicalProject;
//import org.apache.calcite.rel.logical.LogicalTableScan;
//import org.apache.calcite.tools.RelBuilder;
//
//import java.lang.reflect.Field;
//import java.util.function.Predicate;
//
//public class MycatTableLookupRule extends RelRule<MycatTableLookupRule.Config> {
//
//    public MycatTableLookupRule(Config config) {
//        super(config);
//    }
//
//    @Override
//    public void onMatch(RelOptRuleCall call) {
//        Join join = call.rel(0);
//        RelNode left = call.rel(1);
//        MycatView rightView = call.rel(2);
//        RelNode right = rightView.getRelNode();
//        RelBuilder builder = call.builder();
//
//        builder.values(left.getRowType()).push(right).join(join.getJoinType(),  join.getCondition()).projectNamed()
//        .filter(join);
//
//    }
//
//    public interface Config extends RelRule.Config {
//        MycatTableLookupRule.Config LOOKUP_JOIN_TABLE    = EMPTY
//                .as(MycatTableLookupRule.Config.class)
//                .withOperandFor(b0 ->
//                        b0.operand(Join.class).inputs(b1 -> b1.operand(RelNode.class).anyInputs(), b1 -> b1.operand(MycatView.class)
//                                .predicate(t -> t.getRelNode() instanceof TableScan&&t.getDistribution().type() == Distribution.Type.Sharding).anyInputs()))
//                .withDescription("LOOKUP_JOIN")
//                .as(MycatTableLookupRule.Config.class);
//
////        MycatTableLookupRule.Config INDEX_JOIN = EMPTY
////                .as(MycatTableLookupRule.Config.class)
////                .withOperandFor(b0 ->
////                        b0.operand(MycatView.class).predicate(new Predicate<MycatView>() {
////                            @Override
////                            public boolean test(MycatView mycatView) {
////                                mycatView.getDistribution().type() == Distribution.Type.Sharding
////                                return false;
////                            }
////                        }).anyInputs())
////                .withDescription("INDEX_JOIN")
////                .as(MycatTableLookupRule.Config.class);
//        @Override
//        default MycatTableLookupRule toRule() {
//            return new MycatTableLookupRule(this);
//        }
//
//        default MycatTableLookupRule.Config withOperandFor(OperandTransform transform) {
//            withOperandSupplier(transform)
//                    .as(MycatTableLookupRule.Config.class);
//        }
//    }
//}

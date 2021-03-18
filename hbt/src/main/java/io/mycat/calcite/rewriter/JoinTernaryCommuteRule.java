package io.mycat.calcite.rewriter;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;

import java.util.List;

public class JoinTernaryCommuteRule   extends RelRule<JoinTernaryCommuteRule.Config> {

    /**
     * Creates a JoinCommuteRule.
     */
    protected JoinTernaryCommuteRule(JoinTernaryCommuteRule.Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public JoinTernaryCommuteRule(Class<? extends Join> clazz,
                           RelBuilderFactory relBuilderFactory, boolean swapOuter) {
        this(JoinTernaryCommuteRule.Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .as(JoinTernaryCommuteRule.Config.class)
                .withOperandFor(clazz)
                .withSwapOuter(swapOuter));
    }

    @Deprecated // to be removed before 2.0
    public JoinTernaryCommuteRule(Class<? extends Join> clazz,
                                  RelFactories.ProjectFactory projectFactory) {
        this(clazz, RelBuilder.proto(Contexts.of(projectFactory)), false);
    }

    @Deprecated // to be removed before 2.0
    public JoinTernaryCommuteRule(Class<? extends Join> clazz,
                                  RelFactories.ProjectFactory projectFactory, boolean swapOuter) {
        this(clazz, RelBuilder.proto(Contexts.of(projectFactory)), swapOuter);
    }


    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        // SEMI and ANTI join cannot be swapped.
        return join.getJoinType().projectsRight();
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
        Join join = call.rel(0);

//        final RelNode swapped = swap(join, config.isSwapOuter(), call.builder());
//        if (swapped == null) {
//            return;
//        }
//
//        // The result is either a Project or, if the project is trivial, a
//        // raw Join.
//        final Join newJoin =
//                swapped instanceof Join
//                        ? (Join) swapped
//                        : (Join) swapped.getInput(0);
//
//        call.transformTo(swapped);
//
//        // We have converted join='a join b' into swapped='select
//        // a0,a1,a2,b0,b1 from b join a'. Now register that project='select
//        // b0,b1,a0,a1,a2 from (select a0,a1,a2,b0,b1 from b join a)' is the
//        // same as 'b join a'. If we didn't do this, the swap join rule
//        // would fire on the new join, ad infinitum.
//        final RelBuilder relBuilder = call.builder();
//        final List<RexNode> exps =
//                RelOptUtil.createSwappedJoinExprs(newJoin, join, false);
//        relBuilder.push(swapped)
//                .project(exps, newJoin.getRowType().getFieldNames());
//
//        call.getPlanner().ensureRegistered(relBuilder.build(), newJoin);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Walks over an expression, replacing references to fields of the left and
     * right inputs.
     *
     * <p>If the field index is less than leftFieldCount, it must be from the
     * left, and so has rightFieldCount added to it; if the field index is
     * greater than leftFieldCount, it must be from the right, so we subtract
     * leftFieldCount from it.</p>
     */
    private static class VariableReplacer extends RexShuttle {
        private final RexBuilder rexBuilder;
        private final List<RelDataTypeField> leftFields;
        private final List<RelDataTypeField> rightFields;

        VariableReplacer(
                RexBuilder rexBuilder,
                RelDataType leftType,
                RelDataType rightType) {
            this.rexBuilder = rexBuilder;
            this.leftFields = leftType.getFieldList();
            this.rightFields = rightType.getFieldList();
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            int index = inputRef.getIndex();
            if (index < leftFields.size()) {
                // Field came from left side of join. Move it to the right.
                return rexBuilder.makeInputRef(
                        leftFields.get(index).getType(),
                        rightFields.size() + index);
            }
            index -= leftFields.size();
            if (index < rightFields.size()) {
                // Field came from right side of join. Move it to the left.
                return rexBuilder.makeInputRef(
                        rightFields.get(index).getType(),
                        index);
            }
            throw new AssertionError("Bad field offset: index=" + inputRef.getIndex()
                    + ", leftFieldCount=" + leftFields.size()
                    + ", rightFieldCount=" + rightFields.size());
        }
    }

    /**
     * Rule configuration.
     */
    public interface Config extends RelRule.Config {
        JoinTernaryCommuteRule.Config DEFAULT = EMPTY.as(JoinTernaryCommuteRule.Config.class)
                .withOperandFor(Join.class)
                .withSwapOuter(false);

        @Override
        default JoinTernaryCommuteRule toRule() {
            return new JoinTernaryCommuteRule(this);
        }

        /**
         * Defines an operand tree for the given classes.
         */
        default JoinTernaryCommuteRule.Config withOperandFor(Class<? extends Join> joinClass) {
            return withOperandSupplier(b ->
                    b.operand(joinClass).inputs(b1->b1.operand(joinClass).noInputs(),b2->b2.operand(joinClass).noInputs()))
                    .as(JoinTernaryCommuteRule.Config.class);
        }

        /**
         * Whether to swap outer joins.
         */
        @ImmutableBeans.Property
        @ImmutableBeans.BooleanDefault(false)
        boolean isSwapOuter();

        /**
         * Sets {@link #isSwapOuter()}.
         */
        JoinTernaryCommuteRule.Config withSwapOuter(boolean swapOuter);
    }
}
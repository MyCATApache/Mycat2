package io.mycat.optimizer.logical.rules;


import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.logical.MycatFilter;
import io.mycat.optimizer.physical.rules.CheckingUserDefinedFunctionVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Filter} to
 * an {@link MycatFilterRule}.
 */
public class MycatFilterRule extends MycatConverterRule {

    /**
     * Creates a MycatFilterRule.
     */
    public MycatFilterRule(MycatConvention out,
                           RelBuilderFactory relBuilderFactory) {
        super(Filter.class,
                (Predicate<Filter>) r -> !userDefinedFunctionInFilter(r),
                MycatRules.convention, out, relBuilderFactory, "MycatFilterRule");
    }

    private static boolean userDefinedFunctionInFilter(Filter filter) {
        CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
        filter.getCondition().accept(visitor);
        return visitor.containsUserDefinedFunction();
    }

    public RelNode convert(RelNode rel) {
        final Filter filter = (Filter) rel;

        return new MycatFilter(
                rel.getCluster(),
                rel.getTraitSet().replace(out),
                convert(filter.getInput(),
                        filter.getInput().getTraitSet().replace(out)),
                filter.getCondition());
    }
}


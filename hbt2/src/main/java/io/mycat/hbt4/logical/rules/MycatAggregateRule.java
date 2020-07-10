package io.mycat.hbt4.logical.rules;


import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.MycatAggregate;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Aggregate}
 * to a {@link MycatAggregateRule}.
 */
public class MycatAggregateRule extends MycatConverterRule {

    /**
     * Creates a MycatAggregateRule.
     */
    public MycatAggregateRule(MycatConvention out,
                              RelBuilderFactory relBuilderFactory) {
        super(Aggregate.class, (Predicate<RelNode>) r -> true, MycatRules.convention,
                out, relBuilderFactory, "MycatAggregateRule");
    }

    public RelNode convert(RelNode rel) {
        final Aggregate agg = (Aggregate) rel;
        if (agg.getGroupSets().size() != 1) {
            // GROUPING SETS not supported; see
            // [CALCITE-734] Push GROUPING SETS to underlying SQL via Mycat adapter
            return null;
        }
        final RelTraitSet traitSet =
                agg.getTraitSet().replace(out);
        return new MycatAggregate(rel.getCluster(), traitSet,
                convert(agg.getInput(), out), agg.getGroupSet(),
                agg.getGroupSets(), agg.getAggCallList());
    }
}

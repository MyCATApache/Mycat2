package io.mycat.hbt4.physical.rules;


import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.physical.SortAgg;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class SortAggRule extends MycatConverterRule {
    public SortAggRule(final MycatConvention out,
                       RelBuilderFactory relBuilderFactory) {
        super(Aggregate.class, (Predicate<Aggregate>) project ->
                        true,
                MycatRules.convention, out, relBuilderFactory, "SortAggRule");
    }

    public RelNode convert(RelNode rel) {
        final Aggregate aggregate = (Aggregate) rel;
        return new SortAgg(aggregate.getCluster(),
                aggregate.getCluster().traitSetOf(out),aggregate.getInput(),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList());
    }
}
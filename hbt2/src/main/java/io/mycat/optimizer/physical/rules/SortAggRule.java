package io.mycat.optimizer.physical.rules;

import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.physical.SortAgg;
import io.mycat.optimizer.physical.TopN;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
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
package io.mycat.optimizer.physical.rules;

import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.physical.SemiHashJoin;
import io.mycat.optimizer.physical.SortMergeSemiJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class SemiHashJoinRule  extends MycatConverterRule {
    public SemiHashJoinRule(final MycatConvention out,
                                 RelBuilderFactory relBuilderFactory) {
        super(Correlate.class, (Predicate<Correlate>) project ->
                        true,
                MycatRules.convention, out, relBuilderFactory, "SemiHashJoinRule");
    }

    public RelNode convert(RelNode rel) {
        final Correlate join = (Correlate) rel;
        return new SemiHashJoin(join.getCluster(),
                join.getCluster().traitSetOf(out),
                join.getLeft(),
                join.getRight(),
                join.getCorrelationId(),
                join.getRequiredColumns(),
                join.getJoinType());
    }
}
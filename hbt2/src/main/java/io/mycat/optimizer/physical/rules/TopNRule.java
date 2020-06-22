package io.mycat.optimizer.physical.rules;

import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.physical.SortMergeSemiJoin;
import io.mycat.optimizer.physical.TopN;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class TopNRule extends MycatConverterRule {
    public TopNRule(final MycatConvention out,
                    RelBuilderFactory relBuilderFactory) {
        super(Sort.class, (Predicate<Sort>) project ->
                        true,
                MycatRules.convention, out, relBuilderFactory, "TopNRule");
    }

    public RelNode convert(RelNode rel) {
        final Sort sort = (Sort) rel;
        return new TopN(sort.getCluster(),sort.getCluster().traitSetOf(out),sort.getInput(),sort.getCollation(),sort.offset,sort.fetch);
    }
}
package io.mycat.optimizer.physical.rules;

import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.physical.MemSort;
import io.mycat.optimizer.physical.MergeSort;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class MergeSortRule extends MycatConverterRule {
    public MergeSortRule(final MycatConvention out,
                         RelBuilderFactory relBuilderFactory) {
        super(Sort.class, (Predicate<Sort>) project ->
                        true,
                MycatRules.convention, out, relBuilderFactory, "MergeSortRule");
    }

    public RelNode convert(RelNode rel) {
        final Sort Sort = (Sort) rel;
        return new MergeSort(Sort.getCluster(),Sort.getCluster().traitSetOf(out),Sort.getInput(),Sort.getCollation(),Sort.offset,Sort.fetch);
    }
}
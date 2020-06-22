package io.mycat.optimizer.physical.rules;

import com.google.common.collect.ImmutableList;
import io.mycat.optimizer.MycatConvention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class MycatPhysicalRules {
    public static List<RelOptRule> rules(MycatConvention out) {
        return rules(out, RelFactories.LOGICAL_BUILDER);
    }
    public static List<RelOptRule> rules(MycatConvention out,
                                         RelBuilderFactory relBuilderFactory) {
        return ImmutableList.of(
                new BKAJoinRule(out, relBuilderFactory),
                new HashAggRule(out, relBuilderFactory),
                new HashJoinRule(out, relBuilderFactory),
                new MaterializedSemiJoinRule(out, relBuilderFactory),
                new MemSortRule(out, relBuilderFactory),
                new MergeSortRule(out, relBuilderFactory),
                new NestedLoopJoinRule(out, relBuilderFactory),
                new SemiHashJoinRule(out, relBuilderFactory),
                new SortAggRule(out, relBuilderFactory),
                new SortMergeJoinRule(out, relBuilderFactory),
                new SortMergeSemiJoinRule(out, relBuilderFactory),
                new TopNRule(out, relBuilderFactory)
        );
    }
}
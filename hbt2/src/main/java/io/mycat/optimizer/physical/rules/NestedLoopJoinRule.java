package io.mycat.optimizer.physical.rules;

import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.physical.NestedLoopJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class NestedLoopJoinRule extends MycatConverterRule {
    public NestedLoopJoinRule(final MycatConvention out,
                        RelBuilderFactory relBuilderFactory) {
        super(Join.class, (Predicate<Join>) project ->
                        true,
                MycatRules.convention, out, relBuilderFactory, "NestedLoopJoinRule");
    }

    public RelNode convert(RelNode rel) {
        final Join join = (Join) rel;
        return new NestedLoopJoin(join.getCluster(),
                join.getCluster().traitSetOf(out),
                join.getLeft(),
                join.getRight(),
                join.getCondition(),
                join.getJoinType());
    }
}
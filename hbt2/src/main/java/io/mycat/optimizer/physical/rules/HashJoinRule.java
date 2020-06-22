package io.mycat.optimizer.physical.rules;


import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.physical.BKAJoin;
import io.mycat.optimizer.physical.HashJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class HashJoinRule extends MycatConverterRule {
    public HashJoinRule(final MycatConvention out,
                        RelBuilderFactory relBuilderFactory) {
        super(Join.class, (Predicate<Join>) project ->
                        true,
                MycatRules.convention, out, relBuilderFactory, "HashJoinRule");
    }

    public RelNode convert(RelNode rel) {
        final Join join = (Join) rel;
        return new HashJoin(join.getCluster(),
                join.getCluster().traitSetOf(out),
                join.getLeft(),
                join.getRight(),
                join.getCondition(),
                join.getJoinType());
    }
}
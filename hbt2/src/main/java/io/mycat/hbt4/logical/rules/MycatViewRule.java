package io.mycat.hbt4.logical.rules;


import com.google.common.collect.ImmutableList;
import io.mycat.hbt3.View;
import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.MycatQuery;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Project} to
 * an {@link MycatViewRule}.
 */
public class MycatViewRule extends MycatConverterRule {

    /**
     * Creates a MycatProjectRule.
     */
    public MycatViewRule(final MycatConvention out,
                         RelBuilderFactory relBuilderFactory) {
        super(View.class, (Predicate<View>) project ->
                        true,
                MycatRules.convention, out, relBuilderFactory, "MycatViewRule");
    }

    public RelNode convert(RelNode rel) {
        RelOptCluster cluster = rel.getCluster();
        final View view = (View) rel;
        return new MycatQuery((View)view.copy(rel.getTraitSet().replace(out), ImmutableList.of()));
    }
}


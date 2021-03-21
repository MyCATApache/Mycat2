package io.mycat.calcite.rules;

import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatConverterRule;
import io.mycat.calcite.MycatRules;
import io.mycat.calcite.physical.MycatRepeatUnion;
import io.mycat.calcite.physical.MycatWindow;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableWindow;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.tools.RelBuilderFactory;

public class MycatWinodwRule extends MycatConverterRule {


    /** Called from the Config. */
    public MycatWinodwRule(final MycatConvention out,
                           RelBuilderFactory relBuilderFactory) {
        super(LogicalWindow.class,(p)->true,   MycatRules.IN_CONVENTION, out, relBuilderFactory, "MycatWinodwRule");
    }

    @Override public RelNode convert(RelNode rel) {
        final LogicalWindow winAgg = (LogicalWindow) rel;
        final RelTraitSet traitSet =
                winAgg.getTraitSet().replace(MycatConvention.INSTANCE);
        final RelNode child = winAgg.getInput();
        final RelNode convertedChild =
                convert(child,
                        child.getTraitSet().replace(MycatConvention.INSTANCE));
        return new MycatWindow(rel.getCluster(), traitSet, convertedChild,
                winAgg.getConstants(), winAgg.getRowType(), winAgg.groups);
    }
}

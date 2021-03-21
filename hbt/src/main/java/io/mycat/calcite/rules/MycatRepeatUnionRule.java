package io.mycat.calcite.rules;

import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatConverterRule;
import io.mycat.calcite.MycatRules;
import io.mycat.calcite.physical.MycatRepeatUnion;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.tools.RelBuilderFactory;

public class MycatRepeatUnionRule extends MycatConverterRule {


    /** Called from the Config. */
    public MycatRepeatUnionRule(final MycatConvention out,
                                RelBuilderFactory relBuilderFactory) {
        super(LogicalRepeatUnion.class,(p)->true,   MycatRules.IN_CONVENTION, out, relBuilderFactory, "MycatRepeatUnionRule");
    }

    @Override public RelNode convert(RelNode rel) {
        LogicalRepeatUnion union = (LogicalRepeatUnion) rel;
        RelTraitSet traitSet = union.getTraitSet().replace(out);
        RelNode seedRel = union.getSeedRel();
        RelNode iterativeRel = union.getIterativeRel();

        return new MycatRepeatUnion(
                rel.getCluster(),
                traitSet,
                convert(seedRel, seedRel.getTraitSet().replace(out)),
                convert(iterativeRel, iterativeRel.getTraitSet().replace(out)),
                union.all,
                union.iterationLimit);
    }
}

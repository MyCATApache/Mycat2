package io.mycat.optimizer.logical.rules;

import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.logical.MycatUnion;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
     * Rule to convert an {@link Union} to a
     * {@link MycatUnionRule}.
     */
    public  class MycatUnionRule extends MycatConverterRule {

        /**
         * Creates a MycatUnionRule.
         */
        public MycatUnionRule(MycatConvention out,
                              RelBuilderFactory relBuilderFactory) {
            super(Union.class, (Predicate<RelNode>) r -> true, MycatRules.convention, out,
                    relBuilderFactory, "MycatUnionRule");
        }

        public RelNode convert(RelNode rel) {
            final Union union = (Union) rel;
            final RelTraitSet traitSet =
                    union.getTraitSet().replace(out);
            return new MycatUnion(rel.getCluster(), traitSet,
                    convertList(union.getInputs(), out), union.all);
        }
    }

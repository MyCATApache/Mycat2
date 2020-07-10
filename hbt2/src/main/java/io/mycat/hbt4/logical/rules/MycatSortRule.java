package io.mycat.hbt4.logical.rules;


import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.MycatSort;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
     * Rule to convert a {@link Sort} to an
     * {@link MycatSortRule}.
     */
    public  class MycatSortRule extends MycatConverterRule {

        /**
         * Creates a MycatSortRule.
         */
        public MycatSortRule(MycatConvention out,
                             RelBuilderFactory relBuilderFactory) {
            super(Sort.class, (Predicate<RelNode>) r -> true, MycatRules.convention, out,
                    relBuilderFactory, "MycatSortRule");
        }

        public RelNode convert(RelNode rel) {
            return convert((Sort) rel, true);
        }

        /**
         * Converts a {@code Sort} into a {@code MycatSort}.
         *
         * @param sort               Sort operator to convert
         * @param convertInputTraits Whether to convert input to {@code sort}'s
         *                           Mycat convention
         * @return A new MycatSort
         */
        public RelNode convert(Sort sort, boolean convertInputTraits) {
            final RelTraitSet traitSet = sort.getTraitSet().replace(out);

            final RelNode input;
            if (convertInputTraits) {
                final RelTraitSet inputTraitSet = sort.getInput().getTraitSet().replace(out);
                input = convert(sort.getInput(), inputTraitSet);
            } else {
                input = sort.getInput();
            }

            return new MycatSort(sort.getCluster(), traitSet,
                    input, sort.getCollation(), sort.offset, sort.fetch);
        }
    }

  
package io.mycat.calcite.logic;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class MycatToEnumerableConverterRule extends ConverterRule {
    public static final MycatToEnumerableConverterRule INSTANCE =
            new MycatToEnumerableConverterRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates a CassandraToEnumerableConverterRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public MycatToEnumerableConverterRule(
            RelBuilderFactory relBuilderFactory) {
        super(RelNode.class, (Predicate<RelNode>) r -> true,
                Convention.NONE, EnumerableConvention.INSTANCE,
                relBuilderFactory, "MycatToEnumerableConverterRule");
    }
    @Override
    public RelNode convert(RelNode rel) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new MycatToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}
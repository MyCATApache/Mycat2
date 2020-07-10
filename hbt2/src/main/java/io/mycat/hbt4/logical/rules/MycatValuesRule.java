package io.mycat.hbt4.logical.rules;


import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.MycatValues;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule that converts a values operator to Mycat.
 */
public class MycatValuesRule extends MycatConverterRule {
    /**
     * Creates a MycatValuesRule.
     */
    public MycatValuesRule(MycatConvention out,
                           RelBuilderFactory relBuilderFactory) {
        super(Values.class, (Predicate<RelNode>) r -> true, MycatRules.convention,
                out, relBuilderFactory, "MycatValuesRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        Values values = (Values) rel;
        return new MycatValues(values.getCluster(), values.getRowType(),
                values.getTuples(), values.getTraitSet().replace(out));
    }
}
package io.mycat.optimizer.logical.rules;

import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.logical.MycatIntersect;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Intersect}
 * to a {@link MycatIntersectRule}.
 */
public class MycatIntersectRule extends MycatConverterRule {
    /**
     * Creates a MycatIntersectRule.
     */
    public MycatIntersectRule(MycatConvention out,
                               RelBuilderFactory relBuilderFactory) {
        super(Intersect.class, (Predicate<RelNode>) r -> true, MycatRules.convention,
                out, relBuilderFactory, "MycatIntersectRule");
    }

    public RelNode convert(RelNode rel) {
        final Intersect intersect = (Intersect) rel;
        if (intersect.all) {
            return null; // INTERSECT ALL not implemented
        }
        final RelTraitSet traitSet =
                intersect.getTraitSet().replace(out);
        return new MycatIntersect(rel.getCluster(), traitSet,
                convertList(intersect.getInputs(), out), false);
    }
}

package io.mycat.optimizer.logical.rules;

import io.mycat.optimizer.*;
import io.mycat.optimizer.logical.MycatTableModify;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule that converts a table-modification to Mycat.
 */
public  class MycatTableModificationRule extends MycatConverterRule {
    /**
     * Creates a MycatTableModificationRule.
     */
    public MycatTableModificationRule(MycatConvention out,
                                       RelBuilderFactory relBuilderFactory) {
        super(TableModify.class, (Predicate<RelNode>) r -> true,
                MycatRules.convention, out, relBuilderFactory, "MycatTableModificationRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final TableModify modify =
                (TableModify) rel;
        final ModifiableTable modifiableTable =
                modify.getTable().unwrap(ModifiableTable.class);
        if (modifiableTable == null) {
            return null;
        }
        final RelTraitSet traitSet =
                modify.getTraitSet().replace(out);
        return new MycatTableModify(
                modify.getCluster(), traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                convert(modify.getInput(), traitSet),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened());
    }

}

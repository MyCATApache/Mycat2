package io.mycat.optimizer.logical;

import io.mycat.optimizer.*;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;

import java.util.List;

/**
     * Table-modification operator implemented in Mycat convention.
     */
    public  class MycatTableModify extends TableModify implements MycatRel {
    private final Expression expression;

    public MycatTableModify(RelOptCluster cluster,
                            RelTraitSet traitSet,
                            RelOptTable table,
                            Prepare.CatalogReader catalogReader,
                            RelNode input,
                            Operation operation,
                            List<String> updateColumnList,
                            List<RexNode> sourceExpressionList,
                            boolean flattened) {
        super(cluster, traitSet, table, catalogReader, input, operation,
                updateColumnList, sourceExpressionList, flattened);
        assert input.getConvention() instanceof MycatConvention;
        assert getConvention() instanceof MycatConvention;
        final ModifiableTable modifiableTable =
                table.unwrap(ModifiableTable.class);
        if (modifiableTable == null) {
            throw new AssertionError(); // TODO: user error in validator
        }
        this.expression = table.getExpression(Queryable.class);
        if (expression == null) {
            throw new AssertionError(); // TODO: user error in validator
        }
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatTableModify(
                getCluster(), traitSet, getTable(), getCatalogReader(),
                sole(inputs), getOperation(), getUpdateColumnList(),
                getSourceExpressionList(), isFlattened());
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return null;
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return null;
    }
}
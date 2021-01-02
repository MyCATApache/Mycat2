/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.physical;

import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Table-modification operator implemented in Mycat convention.
 */
public class MycatTableModify extends TableModify implements MycatRel {

    public MycatTableModify(RelOptCluster cluster,
                            RelTraitSet traitSet,
                            RelOptTable table,
                            Prepare.CatalogReader catalogReader,
                            RelNode input,
                            Operation operation,
                            List<String> updateColumnList,
                            List<RexNode> sourceExpressionList,
                            boolean flattened) {
        super(cluster, traitSet.replace(MycatConvention.INSTANCE), table, catalogReader, input, operation,
                updateColumnList, sourceExpressionList, flattened);
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

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        throw new UnsupportedOperationException();
    }
}
/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * @author Junwen Chen
 **/
public class MycatRelBuilder extends RelBuilder {

    public MycatRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    public static MycatRelBuilder create(FrameworkConfig config) {
        return Frameworks.withPrepare(config,
                (cluster, relOptSchema, rootSchema, statement) ->
                        new MycatRelBuilder(config.getContext(), cluster, relOptSchema));
    }

    @Override
    public RelBuilder filter(Iterable<CorrelationId> variablesSet,
                             Iterable<? extends RexNode> predicates) {
        ImmutableList<CorrelationId> correlationIds = ImmutableList.copyOf(variablesSet);
        if (correlationIds.isEmpty()) {
            RelNode peek = peek();
            if (peek instanceof Filter) {
                Filter filter = (Filter) peek;
                ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
                ImmutableList<RexNode> rexNodes = builder.add(filter.getCondition()).addAll(predicates).build();
                this.build();
                push(filter.copy(peek.getTraitSet(), ((Filter) peek).getInput(), RexUtil.composeConjunction(getRexBuilder(), rexNodes)));
            }
        } else {
            super.filter(variablesSet, predicates);
        }
        return this;
    }

}
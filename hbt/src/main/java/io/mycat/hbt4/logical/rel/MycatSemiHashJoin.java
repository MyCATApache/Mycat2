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
package io.mycat.hbt4.logical.rel;


import com.google.common.collect.ImmutableSet;
import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

public class MycatSemiHashJoin extends MycatHashJoin implements MycatRel {


    protected MycatSemiHashJoin(RelOptCluster cluster,
                                RelTraitSet traitSet,
                                List<RelHint> hints,
                                RelNode left,
                                RelNode right,
                                RexNode condition,
                                Set<CorrelationId> variablesSet,
                                JoinRelType joinType) {
        super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    }

    public static MycatSemiHashJoin create(List<RelHint> hints, RelNode left, RelNode right, RexNode condition, JoinRelType joinType) {
        RelOptCluster cluster = left.getCluster();
        RelMetadataQuery metadataQuery = cluster.getMetadataQuery();
        return new MycatSemiHashJoin(left.getCluster(),
                cluster.traitSetOf(MycatConvention.INSTANCE)
                .replaceIfs(
                        RelCollationTraitDef.INSTANCE,
                        () -> RelMdCollation.enumerableHashJoin(metadataQuery, left,right,joinType)),
                hints,
                left,
                right,
                condition,
                ImmutableSet.of(),
                joinType
                );
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatSemiHashJoin");
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }


}
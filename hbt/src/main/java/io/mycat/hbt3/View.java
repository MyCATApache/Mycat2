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
package io.mycat.hbt3;

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatSqlDialect;
import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;
import java.util.Map;
import java.util.Objects;


public class View extends AbstractRelNode implements MycatRel {
    RelNode relNode;
    Distribution dataNode;
    final boolean gather;

    public View(RelTraitSet relTrait, RelNode input, Distribution dataNode, boolean gather) {
        super(input.getCluster(), relTrait);
        this.dataNode = Objects.requireNonNull(dataNode);
        this.rowType = input.getRowType();
        this.relNode = input;
        this.traitSet = relTrait;
        this.gather = gather;
    }
    public static View of(RelNode input, Distribution dataNodeInfo) {
        return of(input, dataNodeInfo, false);
    }

    public static View of(RelNode input, Distribution dataNodeInfo, boolean gather) {
        return new View(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo, gather);
    }

    public static View of(RelTraitSet relTrait, RelNode input, Distribution dataNodeInfo, boolean gather) {
        return new View(relTrait.replace(MycatConvention.INSTANCE), input, dataNodeInfo, gather);
    }

    public String getSql() {
        return MycatCalciteSupport.INSTANCE.convertToSql(relNode,
                MycatSqlDialect.DEFAULT, false);
    }

    public String getSql(Map<String, Object> context) {
        return getSql();//@todo
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public Distribution getDataNode() {
        return dataNode;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (!inputs.isEmpty()) {
            throw new AssertionError();
        }
        return new View(traitSet, relNode, dataNode, gather);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("View").into().item("sql", getSql()).ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost relOptCost = super.computeSelfCost(planner, mq);
        return relOptCost;
    }

    public boolean isGather() {
        return gather;
    }

}
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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;


public class View extends AbstractRelNode implements MycatRel {
    RelNode relNode;
    PartInfo dataNode;

    public View(RelTraitSet relTrait, RelNode input, PartInfo dataNode) {
        super(input.getCluster(), input.getTraitSet());
        this.dataNode = dataNode;
        this.rowType = input.getRowType();
        this.relNode = input;
        this.traitSet = relTrait;
    }

    public static View of(RelNode input) {
        return new View(input.getTraitSet().replace(MycatConvention.INSTANCE),input,null);
    }

    public static RelNode of(RelNode input, PartInfo dataNodeInfo) {
        return new View(input.getTraitSet().replace(MycatConvention.INSTANCE),input,dataNodeInfo);
    }


    public String getSql() {
        return MycatCalciteSupport.INSTANCE.convertToSql(relNode,
                MycatSqlDialect.DEFAULT, false);
    }
    public String getSql(Map<String,Object> context) {
        return getSql();//@todo
    }

//    @Override
//    public String toString() {
//        return super.toString() +" "+ dataNode+ " -> " + getSql();
//    }

    public RelNode getRelNode() {
        return relNode;
    }

    public PartInfo getDataNode() {
        return dataNode;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (!inputs.isEmpty()){
            throw new AssertionError();
        }
        return new View(traitSet,relNode, dataNode);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("View").into().item("sql",getSql()).ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}
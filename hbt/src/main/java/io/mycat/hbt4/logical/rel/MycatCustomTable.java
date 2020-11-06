///**
// * Copyright (C) <2020>  <chen junwen>
// * <p>
// * This program is free software: you can redistribute it and/or modify it under the terms of the
// * GNU General Public License as published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * General Public License for more details.
// * <p>
// * You should have received a copy of the GNU General Public License along with this program.  If
// * not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.hbt4.logical.rel;
//
//import io.mycat.hbt4.*;
//import io.mycat.metadata.CustomTableHandler;
//import io.mycat.metadata.QueryBuilder;
//import lombok.Getter;
//import org.apache.calcite.plan.RelOptCluster;
//import org.apache.calcite.plan.RelOptCost;
//import org.apache.calcite.plan.RelOptPlanner;
//import org.apache.calcite.rel.AbstractRelNode;
//import org.apache.calcite.rel.metadata.RelMetadataQuery;
//import org.apache.calcite.rel.type.RelDataType;
//
//@Getter
//public class MycatCustomTable extends AbstractRelNode implements MycatRel {
//
//
//    private final CustomTableHandler tableHandler;
//
//    public MycatCustomTable(RelOptCluster cluster,
//                            RelDataType rowType,
//                            CustomTableHandler  tableHandler) {
//        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
//        this.tableHandler = tableHandler;
//        this.rowType = rowType;
//        this.traitSet = cluster.traitSetOf(MycatConvention.INSTANCE);
//    }
//
//    @Override
//    public ExplainWriter explain(ExplainWriter writer) {
//        return null;
//    }
//
//    @Override
//    public Executor implement(ExecutorImplementor implementor) {
//        return implementor.implement(this);
//    }
//
//    @Override
//    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
//        return super.computeSelfCost(planner, mq);
//    }
//
//    public QueryBuilder createQueryBuilder(){
//        return tableHandler.createQueryBuilder(scan.getCluster());
//    }
//}
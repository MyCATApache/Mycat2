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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.DataNode;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatSqlDialect;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.hbt4.*;
import io.mycat.util.Pair;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.util.SqlString;

import java.util.List;
import java.util.Objects;


public class View extends AbstractRelNode implements MycatRel {
    RelNode relNode;
    Distribution distribution;

    public View(RelTraitSet relTrait, RelNode input, Distribution dataNode) {
        super(input.getCluster(), relTrait);
        this.distribution = Objects.requireNonNull(dataNode);
        this.rowType = input.getRowType();
        this.relNode = input;
        this.traitSet = relTrait;
    }
//    public View(RelTraitSet relTrait, List<RelNode> inputs, Distribution dataNode, boolean gather) {
//        super(inputs.get(0).getCluster(),relTrait);
//        this.distribution = Objects.requireNonNull(dataNode);
//        this.rowType = inputs.get(0).getRowType();
//        this.traitSet = relTrait;
//        this.gather = gather;
//    }
//    public static View of(List<RelNode> input, Distribution dataNodeInfo) {
//        return new View(input.get(0).getTraitSet().replace(MycatConvention.INSTANCE),input,dataNodeInfo,false);
//    }

    public static View of(RelNode input, Distribution dataNodeInfo) {
        return new View(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo);
    }

    public static View of(RelTraitSet relTrait, RelNode input, Distribution dataNodeInfo) {
        return new View(relTrait.replace(MycatConvention.INSTANCE), input, dataNodeInfo);
    }


    public RelNode getRelNode() {
        return relNode;
    }

    public Distribution getDistribution() {
        return distribution;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new View(traitSet, relNode, distribution);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);
        writer.item("relNode",relNode);
        writer.item("distribution",distribution);
//        RelNode relNode = expandToPhyRelNode();
//        if (relNode instanceof Union) {
//            int index = 0;
//            for (RelNode input : relNode.getInputs()) {
//                writer.item("\t\n" + index + "", MycatCalciteSupport.INSTANCE.convertToSql(input, MycatSqlDialect.DEFAULT, false));
//                index++;
//            }
//        } else {
//            writer.item("sql", MycatCalciteSupport.INSTANCE.convertToSql(relNode, MycatSqlDialect.DEFAULT, false));
//        }
//        for (DataNode slice : distribution.getDataNodes()) {
//            RelNode newRelNode = relNode.accept(new RelShuttleImpl() {
//                @Override
//                public RelNode visit(TableScan scan) {
//                    return slice.rewrite(scan);
//                }
//            });
//            String sql = MycatCalciteSupport.INSTANCE.convertToSql(newRelNode, MycatSqlDialect.DEFAULT, false);
//            writer.item(slice.toString(), sql);
//        }

        return writer;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("View").into().item("sql", getSql()).ret();
    }

    public String getSql() {
        return MycatCalciteSupport.INSTANCE.convertToSql(relNode, MycatSqlDialect.DEFAULT, false).getSql();
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


    public RelNode expandToPhyRelNode() {
        if (this.distribution.isPhy() || this.distribution.isBroadCast()) {
            DataNode dataNode = distribution.getDataNodes().iterator().next();
            return applyDataNode(dataNode);
        } else {
            ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
            for (DataNode dataNode : this.distribution.getDataNodes()) {
                builder.add(applyDataNode(dataNode));
            }
            ImmutableList<RelNode> views = builder.build();
            return LogicalUnion.create(views, true);
        }
    }

//    public Map<String, List<View>> expandToPhyView() {
//        if (this.distribution.isSingle() || this.distribution.isBroadCast()) {
//            DataNode dataNode = distribution.getDataNodes().iterator().next();
//            return ImmutableMap.of(dataNode.getTargetName(),
//                    ImmutableList.of(View.of(applyDataNode(dataNode),
//                    Distribution.of(ImmutableList.of(dataNode), "")))
//            );
//        } else {
//            ImmutableMap.Builder<String,List<View>> builder = ImmutableMap.builder();
//            for (DataNode dataNode : this.distribution.getDataNodes()) {
//                builder.add(applyDataNode(dataNode));
//            }
//            ImmutableList<RelNode> views = builder.build();
//            return LogicalUnion.create(views, true);
//        }
//    }

    public ImmutableMultimap<String, SqlString> expandToSql(boolean update, List<Object> params) {
        if (this.distribution.isPhy() || this.distribution.isBroadCast()) {
            DataNode dataNode = distribution.getDataNodes().iterator().next();
            SqlString sql = MycatCalciteSupport.INSTANCE.convertToSql(applyDataNode(dataNode), MycatSqlDialect.DEFAULT, update,params);
            return ImmutableMultimap.of(dataNode.getTargetName(), sql);
        } else {
            ImmutableMultimap.Builder<String, SqlString> builder = ImmutableMultimap.builder();
            for (DataNode dataNode : this.distribution.getDataNodes(params)) {
                SqlString sql = MycatCalciteSupport.INSTANCE.convertToSql(applyDataNode(dataNode), MycatSqlDialect.DEFAULT, update,params);
                builder.put(dataNode.getTargetName(), sql);
            }
            return builder.build();
        }
    }

    public RelNode applyDataNode(DataNode dataNode) {
        return this.relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatLogicTable mycatLogicTable = scan.getTable().unwrap(MycatLogicTable.class);
                if (mycatLogicTable != null) {
                    MycatPhysicalTable physicalTable = new MycatPhysicalTable(mycatLogicTable, dataNode);
                    RelOptTableImpl relOptTable1 = RelOptTableImpl.create(scan.getTable().getRelOptSchema(),
                            scan.getRowType(),
                            physicalTable,
                            ImmutableList.of(dataNode.getSchema(), dataNode.getTable())
                    );
                    return LogicalTableScan.create(scan.getCluster(), relOptTable1, ImmutableList.of());
                }
                return super.visit(scan);
            }
        });
    }



}
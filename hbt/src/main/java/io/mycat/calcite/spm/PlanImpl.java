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
package io.mycat.calcite.spm;

import com.google.common.collect.ImmutableMultimap;
import io.mycat.DrdsSql;
import io.mycat.MycatDataContext;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.executor.MycatInsertExecutor;
import io.mycat.calcite.executor.MycatUpdateExecutor;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.runtime.CodeExecuterContext;
import org.apache.calcite.sql.util.SqlString;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PlanImpl implements Plan {
    private boolean forUpdate;
    private final Type type;
    private final RelOptCost relOptCost;
    private final RelNode physical;
    private final CodeExecuterContext executerContext;


    public static PlanImpl of(RelNode relNode,
                              CodeExecuterContext executerContext, boolean forUpdate) {
        return new PlanImpl(relNode, executerContext, forUpdate);
    }

    public PlanImpl(RelNode relNode,
                    CodeExecuterContext executerContext, boolean forUpdate) {
        this.forUpdate = forUpdate;
        this.type = Type.PHYSICAL;
        RelOptCluster cluster = relNode.getCluster();
        this.relOptCost = relNode.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery());
        this.physical = relNode;
        this.executerContext = executerContext;
    }

    public PlanImpl(MycatInsertRel relNode) {
        this.type = Type.INSERT;
        RelOptCluster cluster = relNode.getCluster();
        this.relOptCost = cluster.getPlanner().getCostFactory().makeZeroCost();
        this.physical = relNode;
        this.executerContext = null;
    }

    public PlanImpl(MycatUpdateRel relNode) {
        this.type = Type.UPDATE;
        RelOptCluster cluster = relNode.getCluster();
        this.relOptCost = cluster.getPlanner().getCostFactory().makeZeroCost();
        this.physical = relNode;
        this.executerContext = null;
    }


    @Override
    public int compareTo(@NotNull Plan o) {
        return this.relOptCost.isLt(o.getRelOptCost()) ? 1 : -1;
    }

    @Override
    public boolean forUpdate() {
        return forUpdate;
    }

    @Override
    public RelOptCost getRelOptCost() {
        return relOptCost;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public CodeExecuterContext getCodeExecuterContext() {
        return executerContext;
    }

    public RelNode getPhysical() {
        return physical;
    }

    @Override
    public List<String> explain(MycatDataContext dataContext, DrdsSql drdsSql) {
        ArrayList<String> list = new ArrayList<>();
        ExplainWriter explainWriter = new ExplainWriter();
        switch (this.type) {
            case PHYSICAL:
                String s = MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(physical);
                list.addAll(Arrays.asList(s.split("\n")));
                physical.accept(new RelShuttleImpl(){
                    @Override
                    protected RelNode visitChildren(RelNode rel) {
                        if (rel instanceof MycatView){
                            ImmutableMultimap<String, SqlString> stringSqlStringImmutableMultimap = ((MycatView) rel).expandToSql(drdsSql.isForUpdate(), drdsSql.getParams());
                            for (Map.Entry<String, SqlString> entry : stringSqlStringImmutableMultimap.entries()) {
                                list.add("\n");
                                list.add(rel.toString());
                                list.add("targetName:"+entry.getKey());
                                list.add("sql:"+entry.getValue());
                            }

                        }else if (rel instanceof MycatTransientSQLTableScan){
                            ((MycatTransientSQLTableScan) rel).explain(explainWriter);
                        }
                        return super.visitChildren(rel);
                    }
                });
                list.add("\n");
                list.addAll(Arrays.asList(getCodeExecuterContext().getCode().split("\n")));
                break;
            case UPDATE: {
                MycatUpdateRel physical = (MycatUpdateRel) this.physical;
                MycatUpdateExecutor.create(physical, dataContext, drdsSql.getParams())
                        .explain(explainWriter);

                break;
            }
            case INSERT: {
                MycatInsertRel physical = (MycatInsertRel) this.physical;
                MycatInsertExecutor.create(dataContext, physical, drdsSql.getParams())
                        .explain(explainWriter);
                break;
            }
        }  for (String s1 : explainWriter.getText().split("\n")) {
            list.add(s1);
        }
        return list;
    }
}
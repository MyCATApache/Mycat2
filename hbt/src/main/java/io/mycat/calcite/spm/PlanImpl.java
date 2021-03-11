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
import io.mycat.calcite.CodeExecuterContext;
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
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.Util;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

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

    public List<String> explain(MycatDataContext dataContext, DrdsSql drdsSql, boolean code) {
        ArrayList<String> list = new ArrayList<>();
        ExplainWriter explainWriter = new ExplainWriter();

        switch (this.type) {
            case PHYSICAL:
                String s = dumpPlan();
                list.addAll(Arrays.asList(s.split("\n")));
                List<SpecificSql> map = specificSql(drdsSql);
                for (SpecificSql specificSql : map) {
                    list.add(specificSql.toString());
                }
                if (code) {
                    list.add("code:");
                    list.addAll(Arrays.asList(getCodeExecuterContext().getCode().split("\n")));
                }
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
            default:
                throw new IllegalStateException("Unexpected value: " + this.type);
        }
        for (String s1 : explainWriter.getText().split("\n")) {
            list.add(s1);
        }
        return list.stream().filter(i -> !i.isEmpty()).collect(Collectors.toList());
    }

    @NotNull
    public String dumpPlan() {
        return MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(physical).replaceAll("\r", "");
    }

    @NotNull
    public List<SpecificSql> specificSql(DrdsSql drdsSql) {
        List<SpecificSql> res = new ArrayList<>();

        physical.accept(new RelShuttleImpl() {
            @Override
            protected RelNode visitChildren(RelNode relNode) {
                String name = MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(relNode).replaceAll("\r", "");
                List< Each> sqls  = new ArrayList<>();
                if (relNode instanceof MycatView) {
                    ImmutableMultimap<String, SqlString> stringSqlStringImmutableMultimap = ((MycatView) relNode).expandToSql(drdsSql.isForUpdate(), drdsSql.getParams());
                    for (Map.Entry<String, SqlString> entry : (stringSqlStringImmutableMultimap.entries())) {
                        SqlString sqlString = new SqlString(
                                entry.getValue().getDialect(),
                                (Util.toLinux(entry.getValue().getSql())),
                                entry.getValue().getDynamicParameters());
                        sqls.add(new Each(entry.getKey(), sqlString.getSql()));
                    }
                    res.add(new SpecificSql(name,((MycatView) relNode).getSql(),sqls));
                } else if (relNode instanceof MycatTransientSQLTableScan) {
                    MycatTransientSQLTableScan rel = (MycatTransientSQLTableScan) relNode;
                    res.add(new SpecificSql(name,rel.getSql(),Collections.singletonList(new Each(rel.getTargetName(), rel.getSql()))));
                }
                return super.visitChildren(relNode);
            }
        });
        return res;
    }
}
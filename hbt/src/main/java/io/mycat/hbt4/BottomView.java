/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatSqlDialect;
import io.mycat.hbt3.AbstractMycatTable;
import lombok.Getter;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
@Getter
public class BottomView extends TableScan implements MycatRel {
    boolean filter;
    boolean join;
    boolean project;
    boolean sort;
    boolean limit;
    boolean filterSubquery;

    BottomView(RelOptCluster cluster, RelTraitSet traitSet,
               RelOptTable relOptTable) {
        super(cluster, traitSet, ImmutableList.of(), relOptTable);
    }


    public static BottomView create(RelOptCluster cluster,
                                    RelOptTable relOptTable) {
        final Table table = relOptTable.unwrap(Table.class);
        final RelTraitSet traitSet =
                cluster.traitSetOf(MycatConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                            if (table != null) {
                                return table.getStatistic().getCollations();
                            }
                            return ImmutableList.of();
                        });
        return new BottomView(cluster, traitSet, relOptTable);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RelDataType deriveRowType() {
        return table.getRowType();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double f;
        f = 0.5;
        return super.computeSelfCost(planner, mq)
                .multiplyBy(f * 0.01d);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return super.copy(traitSet, inputs);
    }

    public boolean isSamePartition(BottomView right) {
        AbstractMycatTable mycatTable = getTable().unwrap(AbstractMycatTable.class);
        return true;
    }

    public String getSql() {
        MycatTransientTable transientTable = table.unwrap(MycatTransientTable.class);
        return MycatCalciteSupport.INSTANCE.convertToSql(transientTable.getRelNode(), MycatSqlDialect.DEFAULT, false);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        MycatTransientTable transientTable = table.unwrap(MycatTransientTable.class);

        String sql = MycatCalciteSupport.INSTANCE.convertToSql(transientTable.getRelNode(), MycatSqlDialect.DEFAULT, false);
        List<DataNode> dataNodes = transientTable.getDataNodes();

        return writer
                .name("BottomView")
                .item("dataNode", dataNodes.stream().map(i -> i.getUniqueName()).collect(Collectors.joining(",")))
                .item("dataNodeCount", dataNodes.size(), dataNodes.size() > 1)
                .item("sql", sql)
                .into()
                .ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return null;
    }

//    @Override
//    public MycatExecutor implement(MycatExecutorImplementor implementor) {
//        MycatTransientTable transientTable = table.unwrap(MycatTransientTable.class);
//        RelNode relNode = transientTable.getRelNode();
//        RelOptCluster cluster = getCluster();
//        RelOptPlanner planner = cluster.getPlanner();
//        RelOptUtil.registerDefaultRules(planner,false,true);
//
//        RelNode relNode1 = planner.changeTraits(relNode, cluster.traitSetOf(BindableConvention.INSTANCE));
//        planner.setRoot(relNode1);
//        InterpretableRel bestExp = (InterpretableRel)planner.findBestExp();
////
////        Nodes.CoreCompiler coreCompiler = new CoreCompilerImpl(objects,cluster);
////        Pair<RelNode, Map<RelNode, Interpreter.NodeInfo>> pair = coreCompiler.visitRoot(bestExp);
////        InterpretableRel.InterpreterImplementor interpreterImplementor = new InterpretableRel.InterpreterImplementor(coreCompiler, null, null);
////      Node implement = bestExp.implement(interpreterImplementor);
//        return null;
//    }

    public RelNode getRelNode() {
        MycatTransientTable transientTable = table.unwrap(MycatTransientTable.class);
        return transientTable.getRelNode();
    }

    public static BottomView makeTransient(RelOptSchema schema, RelNode relNode, List<DataNode> dataNodes) {
        MycatTransientTable mycatTransientTable = new MycatTransientTable(dataNodes, relNode);
        RelOptTableImpl relOptTable1 = RelOptTableImpl.create(schema,
                mycatTransientTable.getRelNode().getRowType(),
                mycatTransientTable
                , ImmutableList.of()
        );
        return BottomView.create(relNode.getCluster(), relOptTable1);
    }

    public static BottomView makeTransient(RelOptTable relOptTable, RelNode relNode, List<DataNode> dataNodes) {
        return makeTransient(relOptTable.getRelOptSchema(), relNode, dataNodes);
    }

    public static BottomView makeTransient(RelOptTable relOptTable, RelNode relNode) {
        MycatTransientTable transientTable = relOptTable.unwrap(MycatTransientTable.class);
        return makeTransient(relOptTable.getRelOptSchema(), relNode, transientTable.getDataNodes());
    }


    List<DataNode> getDataNodes() {
        MycatTransientTable transientTable = table.unwrap(MycatTransientTable.class);
        return transientTable.getDataNodes();
    }
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        ExplainWriter explainWriter = new ExplainWriter();
        explain(explainWriter);
        pw.item("sql", explainWriter.getText());
        return pw;
    }
}

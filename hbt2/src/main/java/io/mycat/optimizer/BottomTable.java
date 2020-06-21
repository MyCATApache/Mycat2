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
package io.mycat.optimizer;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.calcite.MycatCalciteSupport;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
@Getter
public class BottomTable extends TableScan implements MycatRel, BindableRel {

    private RelOptTable table;

    BottomTable(RelOptCluster cluster, RelTraitSet traitSet,
                RelOptTable table) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.table = table;
    }


    public static BottomTable create(RelOptCluster cluster,
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
        return new BottomTable(cluster, traitSet, relOptTable);
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
        return super.computeSelfCost(planner, mq);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return super.copy(traitSet, inputs);
    }

    public boolean isSamePartition(BottomTable right) {
        MycatTable mycatTable = getTable().unwrap(MycatTable.class);
        return true;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        String sql = MycatCalciteSupport.INSTANCE.convertToSql(this, MysqlSqlDialect.DEFAULT, false);
        MycatTable table = this.table.unwrap(MycatTable.class);
        List<DataNode> dataNodes = table.getDataNodes();
        return writer
                .name("BottomView")
                .item("dataNode", dataNodes.stream().map(i -> i.getUniqueName()).collect(Collectors.joining(",")))
                .item("dataNodeCount", dataNodes.size(), dataNodes.size() > 1)
                .item("sql", sql)
                .into()
                .ret();
    }

    @Override
    public MycatExecutor implement(MycatExecutorImplementor implementor) {
        return null;
    }

    @Override
    public Node implement(InterpreterImplementor implementor) {
        return null;
    }

    @Override
    public Class<Object[]> getElementType() {
        return null;
    }

    @Override
    public Enumerable<Object[]> bind(DataContext dataContext) {
        return null;
    }
}

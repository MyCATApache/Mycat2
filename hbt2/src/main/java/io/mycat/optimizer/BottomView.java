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
import io.mycat.calcite.MycatCalciteSupport;
import lombok.Getter;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;
import java.util.Objects;

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
@Getter
public class BottomView extends TableScan implements MycatRel {

    private final RelNode relNode;
    private final RelDataType relDataType;

    BottomView(RelOptCluster cluster, RelTraitSet traitSet,
               RelOptTable table, RelNode relNode) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.relNode =  relNode;
        this.relDataType = relNode==null?table.getRowType():relNode.getRowType();
    }

    /**
     * Creates a BindableTableScan.
     */
    public static BottomView create(RelOptCluster cluster,
                                    RelOptTable relOptTable) {
        return create(cluster, relOptTable,null);
    }

    /**
     * Creates a BindableTableScan.
     */
    public static BottomView create(RelOptCluster cluster,
                                    RelOptTable relOptTable, RelNode relNode) {
        final Table table = relOptTable.unwrap(Table.class);
        final RelTraitSet traitSet =
                cluster.traitSetOf(MycatConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                            if (table != null) {
                                return table.getStatistic().getCollations();
                            }
                            return ImmutableList.of();
                        });
        return new BottomView(cluster, traitSet, relOptTable,
                relNode);
    }


    /** {@inheritDoc} */
    @Override
    public RelDataType deriveRowType() {
      return relDataType;
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        String sql=relNode==null?
                MycatCalciteSupport.INSTANCE.convertToSql(this, MysqlSqlDialect.DEFAULT,false)
                :   MycatCalciteSupport.INSTANCE.convertToSql(relNode, MysqlSqlDialect.DEFAULT,false);
        return super.explainTerms(pw)
                .itemIf("relNode", relNode,relNode!=null)
                .item("sql", sql
                      );

    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
      double f;
      if (relNode!=null){
        f = 0.5;
        return super.computeSelfCost(planner, mq)
                .multiplyBy(f  * 0.01d);
      }else {
        return super.computeSelfCost(planner, mq);
      }
    }

    /** {@inheritDoc} */
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return super.copy(traitSet, inputs);
    }

}

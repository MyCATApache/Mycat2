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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;
import java.util.Objects;

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
public class MycatTableScan extends TableScan implements MycatRel {
  public final MycatTable mycatTable;

  protected MycatTableScan(
      RelOptCluster cluster,
      RelOptTable table,
      MycatTable mycatTable,
      MycatConvention jdbcConvention) {
    super(cluster, cluster.traitSetOf(jdbcConvention), ImmutableList.of(), table);
    this.mycatTable = Objects.requireNonNull(mycatTable);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new MycatTableScan(
        getCluster(), table, mycatTable, (MycatConvention) getConvention());
  }

}

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
package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import io.mycat.Partition;
import io.mycat.MetaClusterCurrent;
import io.mycat.SimpleColumnInfo;
import io.mycat.statistic.StatisticCenter;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class Statistics {
    private static final Logger LOGGER = LoggerFactory.getLogger(Statistics.class);

    public static Statistic createStatistic(Statistic parentStatistic, Partition partition) {
        return new Statistic() {
            @Override
            public Double getRowCount() {
                StatisticCenter statisticCenter = MetaClusterCurrent.wrapper(StatisticCenter.class);
                return statisticCenter.getPhysicsTableRow(partition.getSchema(),
                        partition.getTable(),
                        partition.getTargetName());
            }

            @Override
            public boolean isKey(ImmutableBitSet columns) {
                return parentStatistic.isKey(columns);
            }

            @Override
            public List<ImmutableBitSet> getKeys() {
                return parentStatistic.getKeys();
            }

            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                return parentStatistic.getReferentialConstraints();
            }

            @Override
            public List<RelCollation> getCollations() {
                return parentStatistic.getCollations();
            }

            @Override
            public RelDistribution getDistribution() {
                return parentStatistic.getDistribution();
            }
        };
    }


    public static List<ImmutableBitSet> getIndexes(List<SimpleColumnInfo> columns) {
        List<ImmutableBitSet> immutableBitSets = Collections.emptyList();
        ImmutableList.Builder<ImmutableBitSet> indexes = ImmutableList.builder();
        try {
            int index = 0;
            for (SimpleColumnInfo column : columns) {
                if (column.isIndex()) {
                    indexes.add(ImmutableBitSet.of(index));
                }
                index++;
            }
            immutableBitSets = indexes.build();
        } catch (Throwable e) {
            LOGGER.error("", e);
        }
        return immutableBitSets;
    }

    public static Statistic createStatistic(
            String logicSchemaName,
            String logicTableName,
            List<SimpleColumnInfo> columns) {
        List<ImmutableBitSet> immutableBitSets = getIndexes(columns);
        return new Statistic() {
            public Double getRowCount() {
                StatisticCenter statisticCenter = MetaClusterCurrent.wrapper(StatisticCenter.class);
                return statisticCenter.getLogicTableRow(logicSchemaName, logicTableName);
            }

            public boolean isKey(ImmutableBitSet columns) {
                return immutableBitSets.contains(columns);
            }

            public List<ImmutableBitSet> getKeys() {
                return immutableBitSets;
            }

            public List<RelReferentialConstraint> getReferentialConstraints() {
                return ImmutableList.of();
            }

            public List<RelCollation> getCollations() {
                return ImmutableList.of();
            }

            public RelDistribution getDistribution() {
                return RelDistributionTraitDef.INSTANCE.getDefault();
            }
        };
    }

}
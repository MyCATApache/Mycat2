package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
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

    public static Statistic createStatistic(Statistic parentStatistic, DataNode dataNode) {
        return new Statistic() {
            @Override
            public Double getRowCount() {
                return StatisticCenter.INSTANCE.getPhysicsTableRow(dataNode.getSchema(),
                        dataNode.getTable(),
                        dataNode.getTargetName());
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
                return StatisticCenter.INSTANCE.getLogicTableRow(logicSchemaName, logicTableName);
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
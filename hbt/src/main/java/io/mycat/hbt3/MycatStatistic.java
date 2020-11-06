package io.mycat.hbt3;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class MycatStatistic implements Statistic {
    public Double getRowCount() {
        return null;
    }

    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    public List<ImmutableBitSet> getKeys() {
        return ImmutableList.of();
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

}
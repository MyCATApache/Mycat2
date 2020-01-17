package io.mycat.calcite.logic;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

public class MycatPullupTable extends AbstractRelNode {
    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     */
    public MycatPullupTable(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet);
    }
}
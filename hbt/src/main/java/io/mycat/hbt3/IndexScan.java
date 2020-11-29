package io.mycat.hbt3;

import io.mycat.hbt4.*;
import io.mycat.hbt4.executor.ScanExecutor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;

public class IndexScan extends AbstractRelNode implements MycatRel {
    private final RelNode input;
    private List<Object> params;

    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param traitSet
     * @param cluster
     * @param input
     */
    public IndexScan(RelOptCluster cluster, RelNode input) {
        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
        this.rowType = input.getRowType();
        this.input = input;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer;
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    public void setParams(List<Object> params) {
        this.params = params;
    }

    public RelNode getInput() {
        return input;
    }

    public List<Object> getParams() {
        return params;
    }
}

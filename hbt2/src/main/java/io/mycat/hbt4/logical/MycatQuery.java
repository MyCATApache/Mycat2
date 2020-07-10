package io.mycat.hbt4.logical;

import io.mycat.hbt3.View;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExecutorImplementor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.hbt4.MycatRel;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

@Getter
public class MycatQuery extends AbstractRelNode implements MycatRel {


    private final View view;

    public MycatQuery(View view) {
        super(view.getCluster(),view.getTraitSet());
        this.view = view;
        this.rowType = view.getRowType();
        this.traitSet = view.getTraitSet();
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return null;
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }
}
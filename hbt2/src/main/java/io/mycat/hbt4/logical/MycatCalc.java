package io.mycat.hbt4.logical;

import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;

import java.util.List;

;

/**
 * Calc operator implemented in Mycat convention.
 *
 * @see Calc
 */
public class MycatCalc extends SingleRel implements MycatRel {
    private final RexProgram program;

    public MycatCalc(RelOptCluster cluster,
                     RelTraitSet traitSet,
                     RelNode input,
                     RexProgram program) {
        super(cluster, traitSet, input);
        assert getConvention() instanceof MycatConvention;
        this.program = program;
        this.rowType = program.getOutputRowType();
    }

    public RelWriter explainTerms(RelWriter pw) {
        return program.explainCalc(super.explainTerms(pw));
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return RelMdUtil.estimateFilteredRows(getInput(), program, mq);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double dRows = mq.getRowCount(this);
        double dCpu = mq.getRowCount(getInput())
                * program.getExprCount();
        double dIo = 0;
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatCalc(getCluster(), traitSet, sole(inputs), program);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatCalc").item("program", this.program).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}

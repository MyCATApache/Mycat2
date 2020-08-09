/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.hbt4.logical.rel;

import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;

import java.util.List;

/**
 * Calc operator implemented in Mycat convention.
 */
public class MycatCalc extends SingleRel implements MycatRel {
    private final RexProgram program;

    protected MycatCalc(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelNode input,
                        RexProgram program) {
        super(cluster, traitSet, input);
        assert getConvention() instanceof MycatConvention;
        this.program = program;
        this.rowType = program.getOutputRowType();
    }

    public static MycatCalc create(
            RelTraitSet traitSet,
            RelNode input,
            RexProgram program) {
        RelOptCluster cluster = input.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        traitSet = traitSet.replace(MycatConvention.INSTANCE);
        traitSet = traitSet.replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.calc(mq, input, program));
        return new MycatCalc(
                cluster,
                traitSet,
                input,
                program
        );
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

    public RexProgram getProgram() {
        return program;
    }
}

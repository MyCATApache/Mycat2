package io.mycat.calcite.localrel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexProgram;

import java.util.List;

public class LocalCalc extends Calc implements LocalRel  {
    protected LocalCalc(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode child, RexProgram program) {
        super(cluster, traits, hints, child, program);
    }

    @Override
    public LocalCalc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new LocalCalc(getCluster(),traitSet,getHints(),child,program);
    }
}

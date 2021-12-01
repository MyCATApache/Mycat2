package io.mycat.calcite.localrel;

import com.google.common.collect.ImmutableList;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.calcite.MycatRelDataTypeUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class LocalCalc extends Calc implements LocalRel  {
    protected LocalCalc(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode child, RexProgram program) {
        super(cluster, traits.replace(LocalConvention.INSTANCE), hints, child, program);
    }
    public LocalCalc(RelInput input) {
        this(input.getCluster(),
                input.getTraitSet(),
                ImmutableList.of(),
                input.getInput(),
                RexProgram.create(input));
    }

    public static LocalCalc create(Calc calc, RelNode input) {
        return new LocalCalc(calc.getCluster(), calc.getTraitSet(), calc.getHints(), input, calc.getProgram());
    }
    @Override
    public LocalCalc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new LocalCalc(getCluster(),traitSet,getHints(),child,program);
    }
    @Override
    public MycatRelDataType getMycatRelDataType() {
        return MycatRelDataTypeUtil.getMycatRelDataType(getRowType());
    }
}

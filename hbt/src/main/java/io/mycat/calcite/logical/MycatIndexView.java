package io.mycat.calcite.logical;

import io.mycat.calcite.rewriter.Distribution;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

public class MycatIndexView extends MycatView {
    public MycatIndexView(RelTraitSet relTrait, RelNode input, Distribution dataNode) {
        super(relTrait, input, dataNode);
    }

    public MycatIndexView(RelInput relInput) {
        super(relInput);
    }

    public MycatIndexView(RelTraitSet relTrait, RelNode input, Distribution dataNode, RexNode conditions) {
        super(relTrait, input, dataNode, conditions);
    }

}

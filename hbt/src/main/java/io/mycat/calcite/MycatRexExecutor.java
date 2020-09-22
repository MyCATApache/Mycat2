package io.mycat.calcite;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class MycatRexExecutor implements RexExecutor {
    public static final  MycatRexExecutor INSTANCE = new MycatRexExecutor();
    @Override
    public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {

    }
}
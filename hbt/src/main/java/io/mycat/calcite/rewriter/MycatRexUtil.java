package io.mycat.calcite.rewriter;

import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class MycatRexUtil {
    public static Object resolveParam(RexNode rexNode, List<Object> params) {
        if (rexNode instanceof RexDynamicParam) {
            int index = ((RexDynamicParam) rexNode).getIndex();
            return params.get(index);
        }
        if (rexNode instanceof RexLiteral) {
            return ((RexLiteral) rexNode).getValue();
        }
        return rexNode;
    }
}

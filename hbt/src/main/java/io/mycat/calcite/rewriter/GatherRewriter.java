package io.mycat.calcite.rewriter;

import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatGather;
import io.mycat.calcite.physical.MycatMergeSort;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

public class GatherRewriter  extends RelShuttleImpl {

    @Override
    public RelNode visit(RelNode other) {
        RelNode relNode = visit(other);
        if (relNode instanceof MycatView) {
           return MycatGather.create(relNode);
        }else if (relNode instanceof MycatMergeSort) {
            return MycatGather.create(relNode);
        }
        return super.visit(other);
    }
}

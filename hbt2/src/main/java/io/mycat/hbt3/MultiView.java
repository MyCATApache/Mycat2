package io.mycat.hbt3;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class MultiView extends View  {

    public MultiView(RelTraitSet relTrait, RelNode input, DataNodeInfo dataNode) {
        super(relTrait, input, dataNode);
    }
}
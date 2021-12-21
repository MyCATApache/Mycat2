package io.ordinate.engine.factory;

import io.mycat.calcite.logical.MycatView;
import io.ordinate.engine.physicalplan.PhysicalPlan;

public class ViewFactory implements Factory{
    private  MycatView mycatRel;

    public ViewFactory(MycatView mycatRel) {
        this.mycatRel = mycatRel;
    }

    public static ViewFactory of(MycatView mycatRel) {
        return new ViewFactory(mycatRel);
    }

    @Override
    public PhysicalPlan create(ComplierContext context) {
        return null;
    }
}

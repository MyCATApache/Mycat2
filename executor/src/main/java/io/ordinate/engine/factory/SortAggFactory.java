package io.ordinate.engine.factory;

import io.mycat.calcite.physical.MycatSortAgg;
import io.ordinate.engine.physicalplan.PhysicalPlan;

public class SortAggFactory implements Factory{
    private MycatSortAgg mycatRel;
    private Factory inputFactory;

    public SortAggFactory(MycatSortAgg mycatRel, Factory inputFactory) {
        this.mycatRel = mycatRel;
        this.inputFactory = inputFactory;
    }

    public static Factory of(MycatSortAgg mycatRel, Factory inputFactory) {
        return new SortAggFactory(mycatRel,inputFactory);
    }

    @Override
    public PhysicalPlan create(ComplierContext context) {
        PhysicalPlan physicalPlan = this.inputFactory.create(context);
        return null;
    }
}

package io.ordinate.engine.factory;

import io.mycat.calcite.physical.MycatHashAggregate;
import io.ordinate.engine.physicalplan.PhysicalPlan;

public class HashAggFactory implements Factory{
    final MycatHashAggregate mycatRel;
   final Factory inputFactory;

    public HashAggFactory(MycatHashAggregate mycatRel, Factory inputFactory) {
        this.mycatRel = mycatRel;
        this.inputFactory = inputFactory;
    }

    public static HashAggFactory of(MycatHashAggregate mycatRel, Factory inputFactory) {
        return new HashAggFactory(mycatRel,inputFactory);
    }

    @Override
    public PhysicalPlan create(ComplierContext context) {
        PhysicalPlan physicalPlan = inputFactory.create(context);

        return null;
    }
}

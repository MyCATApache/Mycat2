package io.ordinate.engine.factory;

import io.mycat.calcite.physical.MycatFilter;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.physicalplan.FilterPlan;
import io.ordinate.engine.physicalplan.PhysicalPlan;

public class FilterFactory implements Factory {
    final MycatFilter mycatRel;
    final Factory inputFactory;

    public FilterFactory(MycatFilter mycatRel, Factory inputFactory) {
        this.mycatRel = mycatRel;
        this.inputFactory = inputFactory;
    }

    public static FilterFactory of(MycatFilter mycatRel, Factory inputFactory) {
        return new FilterFactory(mycatRel, inputFactory);
    }

    @Override
    public PhysicalPlan create(ComplierContext context) {
        PhysicalPlan physicalPlan = inputFactory.create(context);
        return new FilterPlan(
                physicalPlan,
                context.convertRex(mycatRel.getCondition()),
                physicalPlan.schema()
        );
    }
}

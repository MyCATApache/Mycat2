package io.ordinate.engine.factory;

import io.mycat.calcite.physical.MycatMemSort;
import io.ordinate.engine.builder.CalciteCompiler;
import io.ordinate.engine.physicalplan.PhysicalPlan;
import io.ordinate.engine.physicalplan.SortPlan;

public class MenSortFactory implements Factory {
    private MycatMemSort mycatRel;
    private Factory inputFactory;

    public MenSortFactory(MycatMemSort mycatRel, Factory inputFactory) {
        this.mycatRel = mycatRel;
        this.inputFactory = inputFactory;
    }

    public static Factory of(MycatMemSort mycatRel, Factory inputFactory) {
        return new MenSortFactory(mycatRel,inputFactory);
    }

    @Override
    public PhysicalPlan create(ComplierContext context) {
        PhysicalPlan physicalPlan = inputFactory.create(context);
        return SortPlan.create(physicalPlan, CalciteCompiler.getPhysicalSortProperties(mycatRel));
    }
}

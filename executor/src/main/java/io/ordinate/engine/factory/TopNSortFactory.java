package io.ordinate.engine.factory;

import io.mycat.calcite.physical.MycatMemSort;
import io.mycat.calcite.physical.MycatTopN;
import io.ordinate.engine.builder.CalciteCompiler;
import io.ordinate.engine.physicalplan.PhysicalPlan;
import io.ordinate.engine.physicalplan.SortPlan;

public class TopNSortFactory implements Factory{
    private final MycatTopN mycatRel;
    private final Factory inputFactory;

    public TopNSortFactory(MycatTopN mycatRel, Factory inputFactory) {
        this.mycatRel = mycatRel;
        this.inputFactory = inputFactory;
    }

    public static Factory of(MycatTopN mycatRel, Factory inputFactory) {
        return new TopNSortFactory(mycatRel,inputFactory);
    }
    @Override
    public PhysicalPlan create(ComplierContext context) {
        PhysicalPlan physicalPlan = inputFactory.create(context);
        return SortPlan.create(physicalPlan, CalciteCompiler.getPhysicalSortProperties(mycatRel));
    }
}

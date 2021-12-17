package io.ordinate.engine.factory;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.physical.MycatCalc;
import io.ordinate.engine.physicalplan.PhysicalPlan;
import io.ordinate.engine.vector.VectorExpression;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

public class CaclFactory implements Factory{
    final MycatCalc mycatRel;
    final Factory inputFactory;

    public CaclFactory(MycatCalc mycatRel, Factory inputFactory) {
        this.mycatRel = mycatRel;
        this.inputFactory = inputFactory;
    }

    public static Factory of(MycatCalc mycatRel, Factory inputFactory) {
        return new CaclFactory(mycatRel,inputFactory);
    }

    @Override
    public PhysicalPlan create(ComplierContext context) {
        return null;
    }
}

package io.ordinate.engine.physicalplan;

import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public class MycatViewPhysicalPlanImpl implements PhysicalPlan {
    @Override
    public Schema schema() {
        return null;
    }

    @Override
    public List<PhysicalPlan> children() {
        return null;
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        return null;
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {

    }
}

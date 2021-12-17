package io.ordinate.engine.physicalplan;

import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.util.ResultWriterUtil;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.List;

public class UnionAllObjectArray implements PhysicalPlan {
    final Schema schema;
    final List<Observable<Object[]>> inputs;

    public UnionAllObjectArray(Schema schema, List<Observable<Object[]>> inputs) {
        this.schema = schema;
        this.inputs = inputs;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.emptyList();
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        return Observable.fromIterable(inputs).flatMap(vector -> {
            return ResultWriterUtil.convertToVector(schema, vector);
        });
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {

    }
}

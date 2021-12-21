package io.ordinate.engine.physicalplan;

import io.ordinate.engine.builder.PhysicalSortProperty;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.List;

public class MergeSort implements PhysicalPlan{
  final   Schema schema;
    final  List<PhysicalPlan> inputs;
    private List<PhysicalSortProperty> physicalSortProperties;
    private int offset;
    private int fetch;

    public MergeSort(Schema schema, List<PhysicalPlan> inputs, List<PhysicalSortProperty> physicalSortProperties, int offset , int fetch) {
        this.schema = schema;
        this.inputs = inputs;
        this.physicalSortProperties = physicalSortProperties;
        this.offset = offset;
        this.fetch = fetch;
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
        return null;
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {

    }
}

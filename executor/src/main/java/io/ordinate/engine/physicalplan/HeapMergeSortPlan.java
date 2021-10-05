/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.physicalplan;

import hu.akarnokd.rxjava3.operators.Flowables;
import io.ordinate.engine.builder.PhysicalSortProperty;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Comparator;
import java.util.List;

public class HeapMergeSortPlan implements PhysicalPlan {
    private  List<PhysicalPlan> inputs;
    private List<PhysicalSortProperty> physicalSortProperties;
    private IntFunction offset;
    private IntFunction fetch;

    public HeapMergeSortPlan(List<PhysicalPlan>  inputs, List<PhysicalSortProperty> physicalSortProperties, IntFunction offset, IntFunction fetch) {
        this.inputs = inputs;
        this.physicalSortProperties = physicalSortProperties;
        this.offset = offset;
        this.fetch = fetch;
    }

    @Override
    public Schema schema() {
        return inputs.get(0).schema();
    }

    @Override
    public List<PhysicalPlan> children() {
        return inputs;
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        Comparator<Record> recordComparator = PhysicalSortProperty.getRecordComparator(physicalSortProperties);
        Flowable[] midPans = new Flowable[inputs.size()];
        for (int index = 0; index <inputs.size(); index++) {
            midPans[index]=  OutputLinq4jPhysicalPlan.create(inputs.get(index)).executeToObject(rootContext).toFlowable(BackpressureStrategy.BUFFER);
        }


        Flowable<Record> flowable = Flowables.orderedMerge(recordComparator,midPans).skip(offset.getInt(null)).take(fetch.getInt(null));
        return InputRecordPhysicalPlan.create(schema(),flowable.toObservable()).execute(rootContext);
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        throw new UnsupportedOperationException();
    }
}

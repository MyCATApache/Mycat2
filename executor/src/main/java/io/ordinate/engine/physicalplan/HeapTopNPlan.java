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

import io.ordinate.engine.builder.PhysicalSortProperty;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.record.RecordImpl;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class HeapTopNPlan implements PhysicalPlan {
    private  PhysicalPlan input;
    private List<PhysicalSortProperty> physicalSortProperties;
    private IntFunction offset;
    private IntFunction fetch;

    public HeapTopNPlan(PhysicalPlan input, List<PhysicalSortProperty> physicalSortProperties, IntFunction offset, IntFunction fetch) {
        this.input = input;
        this.physicalSortProperties = physicalSortProperties;
        this.offset = offset;
        this.fetch = fetch;
    }

    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.singletonList(input);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        Comparator<Record> recordComparator;
        if (physicalSortProperties.size()==1){
            recordComparator = physicalSortProperties.get(0).evaluateToSortComparator();
        }else {
            recordComparator = physicalSortProperties.get(0).evaluateToSortComparator();
            for (PhysicalSortProperty physicalSortProperty : physicalSortProperties.subList(1, physicalSortProperties.size())) {
                recordComparator= recordComparator.thenComparing( physicalSortProperty.evaluateToSortComparator());
            }
        }
        OutputLinq4jPhysicalPlan midPlan = OutputLinq4jPhysicalPlan.create(input);
        Observable<Object[]> observable = midPlan.executeToObject(rootContext);
        @NonNull Iterable<Record> objects = observable.map(i-> RecordImpl.create(i)).blockingIterable();
        Enumerable<Record> records = EnumerableDefaults.orderBy(Linq4j.asEnumerable(objects), i -> i, recordComparator, offset.getInt(null), fetch.getInt(null));

        return InputRecordPhysicalPlan.create(schema(), Observable.fromIterable( records)).execute(rootContext);
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }
}

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

import io.ordinate.engine.function.Function;
import io.ordinate.engine.record.JoinRecord;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.record.RecordImpl;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;

import java.util.Arrays;
import java.util.List;

public class HeapNLJJoinNPlan implements PhysicalPlan {
    PhysicalPlan left;
    PhysicalPlan right;
    Function predicate;
    Schema schema;
    JoinType joinType;
    public HeapNLJJoinNPlan(
            PhysicalPlan left,
            PhysicalPlan right,
            Function predicate,
            Schema schema,
            JoinType joinType
    ) {
        this.left = left;
        this.right = right;
        this.predicate = predicate;
        this.schema = schema;
        this.joinType = joinType;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return Arrays.asList(left,right);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        OutputLinq4jPhysicalPlan leftObjectPlan = OutputLinq4jPhysicalPlan.create(left);
        Observable<Object[]> leftObservable = leftObjectPlan.executeToObject(rootContext);
        @NonNull Iterable<Record> leftObjects = leftObservable.map(i-> RecordImpl.create(i)).blockingIterable();


        OutputLinq4jPhysicalPlan rightObjectPlan = OutputLinq4jPhysicalPlan.create(left);
        Observable<Object[]> rightObservable = leftObjectPlan.executeToObject(rootContext);
        @NonNull Iterable<Record> rightObjects = leftObservable.map(i-> RecordImpl.create(i)).blockingIterable();
        int leftColumnCount = left.schema().getFields().size();
        Enumerable<Record> records = EnumerableDefaults.nestedLoopJoin(
                Linq4j.asEnumerable(leftObjects),
                Linq4j.asEnumerable(rightObjects),
                new Predicate2<Record, Record>() {
                    @Override
                    public boolean apply(Record v0, Record v1) {
                        JoinRecord joinRecord = JoinRecord.create(v0, v1, leftColumnCount);
                        return predicate.getBooleanType(joinRecord);
                    }
                }, new Function2<Record, Record, Record>() {
                    @Override
                    public Record apply(Record v0, Record v1) {
                        JoinRecord joinRecord = JoinRecord.create(v0, v1, leftColumnCount);
                        return joinRecord;
                    }
                }, joinType
        );
       return InputRecordPhysicalPlan.create(schema(),Observable.fromIterable(records)).execute(rootContext);
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
    throw new UnsupportedOperationException();
    }
}

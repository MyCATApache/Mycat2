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

import io.ordinate.engine.builder.RexConverter;
import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.record.*;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.schema.IntInnerType;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.*;
import org.apache.calcite.util.ImmutableIntList;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class HeapHashJoinNPlan implements PhysicalPlan {
    PhysicalPlan left;
    PhysicalPlan right;
    ImmutableIntList leftKeys;
    ImmutableIntList rightKeys;
    Function predicate;
    Schema schema;
    JoinType joinType;

    public HeapHashJoinNPlan(
            PhysicalPlan left,
            PhysicalPlan right,
            ImmutableIntList leftKeys,
            ImmutableIntList rightKeys,
            Function predicate,
            Schema schema,
            JoinType joinType
    ) {
        this.left = left;
        this.right = right;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
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
        return Arrays.asList(left, right);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        OutputLinq4jPhysicalPlan leftObjectPlan = OutputLinq4jPhysicalPlan.create(left);
        Observable<Object[]> leftObservable = leftObjectPlan.executeToObject(rootContext);
        @NonNull Iterable<Record> leftObjects = leftObservable.map(i -> RecordImpl.create(i)).blockingIterable();
        @NonNull Iterable<Record> rightObjects = leftObservable.map(i -> RecordImpl.create(i)).blockingIterable();
        int leftColumnCount = left.schema().getFields().size();

        RecordSink leftKeyRecordSink = RecordSinkFactory.INSTANCE.buildRecordSink(InnerType.fromSchemaToIntInnerTypes(this.leftKeys, left.schema()));
        RecordSink rightKeyRecordSink = RecordSinkFactory.INSTANCE.buildRecordSink(InnerType.fromSchemaToIntInnerTypes(this.rightKeys, right.schema()));



        Enumerable<Record> outer =Linq4j.asEnumerable( leftObjects);
        Enumerable<Record> inner = Linq4j.asEnumerable( rightObjects);
        Function1<Record, Object> outerKeySelector = a0 -> {
            RecordSetter recordSetter = SimpleRecordSetterImpl.create(leftKeys.size());
            leftKeyRecordSink.copy(a0, recordSetter);
            return recordSetter;
        };
        Function1<Record, Object> innerKeySelector = a0 -> {
            RecordSetter recordSetter = SimpleRecordSetterImpl.create(rightKeys.size());
            rightKeyRecordSink.copy(a0, recordSetter);
            return recordSetter;
        };
        Function2<Record, Record, Record> resultSelector = (v0, v1) -> JoinRecord.create(v0,v1,leftColumnCount);
        EqualityComparer<Object> comparer = new EqualityComparer<Object>() {
            @Override
            public boolean equal(Object v1, Object v2) {
                return Objects.equals(v1,v2);
            }

            @Override
            public int hashCode(Object record) {
                return Objects.hashCode(record);
            }
        };
        Enumerable < Record > records = EnumerableDefaults.hashJoin(
                outer,
                inner,
                outerKeySelector,innerKeySelector ,resultSelector
                ,comparer,joinType.generatesNullsOnLeft(),joinType.generatesNullsOnRight()).where(v0 -> predicate.getBooleanType(v0));
        return InputRecordPhysicalPlan.create(schema(), Observable.fromIterable(records)).execute(rootContext);
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        throw new UnsupportedOperationException();
    }
}

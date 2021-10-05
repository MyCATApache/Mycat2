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
import io.ordinate.engine.function.Function;
import io.ordinate.engine.record.*;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.util.ImmutableIntList;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class MergeJoinPlan implements PhysicalPlan {
    PhysicalPlan left;
    PhysicalPlan right;
    List<PhysicalSortProperty>  joinKeys;
    ImmutableIntList leftKeys;
    ImmutableIntList rightKeys;
    Function predicate;
    Schema schema;
    JoinType joinType;

    public MergeJoinPlan(
            PhysicalPlan left,
            PhysicalPlan right,
            ImmutableIntList  leftKeys,
            ImmutableIntList rightKeys,
            List<PhysicalSortProperty>  joinKeys,
            Function predicate,
            Schema schema,
            JoinType joinType
    ) {
        this.left = left;
        this.right = right;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.joinKeys = joinKeys;
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

        final Enumerable<Record> outer=Linq4j.asEnumerable( leftObjects);
        final Enumerable<Record> inner= Linq4j.asEnumerable( rightObjects);
        final Function1<Record, Record> outerKeySelector = a0 -> a0;
        final Function1<Record, Record> innerKeySelector= a0 -> a0;
        final Predicate2<Record, Record> extraPredicate = (v0, v1) -> predicate.getBooleanType( JoinRecord.create(v0,v1,leftColumnCount));

        Function2<Record, Record, Record> resultSelector = (v0, v1) -> JoinRecord.create(v0,v1,leftColumnCount);

        final JoinType joinType  = this.joinType;
        final Comparator<Record> comparator =  PhysicalSortProperty.getRecordComparator(joinKeys);

        Enumerable < Record > records = EnumerableDefaults.mergeJoin(
                outer,
                inner,
                outerKeySelector,innerKeySelector ,extraPredicate,resultSelector,joinType
                ,comparator);
        return InputRecordPhysicalPlan.create(schema(), Observable.fromIterable(records)).execute(rootContext);
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        throw new UnsupportedOperationException();
    }
}

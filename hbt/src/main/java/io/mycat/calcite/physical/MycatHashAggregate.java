/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.physical;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.enumerable.impl.AggResultContextImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;


/**
 * Aggregate operator implemented in Mycat convention.
 */
public class MycatHashAggregate extends EnumerableAggregateBase implements MycatRel {
    protected MycatHashAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof MycatConvention;
    }
    public static MycatHashAggregate create(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        traitSet = traitSet.replace(MycatConvention.INSTANCE);
        return new MycatHashAggregate(input.getCluster(),traitSet,input,groupSet,groupSets,aggCalls);

    }
    @Override
    public MycatHashAggregate copy(RelTraitSet traitSet, RelNode input,
                                   ImmutableBitSet groupSet,
                                   List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new MycatHashAggregate(getCluster(), traitSet, input,
                groupSet, groupSets, aggCalls);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatAggregate").item("groupSets", groupSets).item("aggCalls", aggCalls).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
    }


    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableRel child = (EnumerableRel) getInput();
        final Result result = implementor.visitChild(this, 0, child, pref);
        Expression childExp =
                builder.append(
                        "child",
                        result.block);

        final PhysType physType =
                PhysTypeImpl.of(
                        typeFactory, getRowType(), pref.preferCustom());

        // final Enumerable<Employee> child = <<child adapter>>;
        // Function1<Employee, Integer> keySelector =
        //     new Function1<Employee, Integer>() {
        //         public Integer apply(Employee a0) {
        //             return a0.deptno;
        //         }
        //     };
        // Function1<Employee, Object[]> accumulatorInitializer =
        //     new Function1<Employee, Object[]>() {
        //         public Object[] apply(Employee a0) {
        //             return new Object[] {0, 0};
        //         }
        //     };
        // Function2<Object[], Employee, Object[]> accumulatorAdder =
        //     new Function2<Object[], Employee, Object[]>() {
        //         public Object[] apply(Object[] a1, Employee a0) {
        //              a1[0] = ((Integer) a1[0]) + 1;
        //              a1[1] = ((Integer) a1[1]) + a0.salary;
        //             return a1;
        //         }
        //     };
        // Function2<Integer, Object[], Object[]> resultSelector =
        //     new Function2<Integer, Object[], Object[]>() {
        //         public Object[] apply(Integer a0, Object[] a1) {
        //             return new Object[] { a0, a1[0], a1[1] };
        //         }
        //     };
        // return childEnumerable
        //     .groupBy(
        //        keySelector, accumulatorInitializer, accumulatorAdder,
        //        resultSelector);
        //
        // or, if key has 0 columns,
        //
        // return childEnumerable
        //     .aggregate(
        //       accumulatorInitializer.apply(),
        //       accumulatorAdder,
        //       resultSelector);
        //
        // with a slightly different resultSelector; or if there are no aggregate
        // functions
        //
        // final Enumerable<Employee> child = <<child adapter>>;
        // Function1<Employee, Integer> keySelector =
        //     new Function1<Employee, Integer>() {
        //         public Integer apply(Employee a0) {
        //             return a0.deptno;
        //         }
        //     };
        // EqualityComparer<Employee> equalityComparer =
        //     new EqualityComparer<Employee>() {
        //         boolean equal(Employee a0, Employee a1) {
        //             return a0.deptno;
        //         }
        //     };
        // return child
        //     .distinct(equalityComparer);

        final PhysType inputPhysType = result.physType;

        ParameterExpression parameter =
                Expressions.parameter(inputPhysType.getJavaRowType(), "a0");

        final PhysType keyPhysType =
                inputPhysType.project(groupSet.asList(), getGroupType() != Group.SIMPLE,
                        JavaRowFormat.LIST);
        final int groupCount = getGroupCount();

        final List<AggImpState> aggs = new ArrayList<>(aggCalls.size());
        for (Ord<AggregateCall> call : Ord.zip(aggCalls)) {
            aggs.add(new AggImpState(call.i, call.e, false));
        }

        // Function0<Object[]> accumulatorInitializer =
        //     new Function0<Object[]>() {
        //         public Object[] apply() {
        //             return new Object[] {0, 0};
        //         }
        //     };
        final List<Expression> initExpressions = new ArrayList<>();
        final BlockBuilder initBlock = new BlockBuilder();

        final List<Type> aggStateTypes = createAggStateTypes(
                initExpressions, initBlock, aggs, typeFactory);

        final PhysType accPhysType =
                PhysTypeImpl.of(typeFactory,
                        typeFactory.createSyntheticType(aggStateTypes));

        declareParentAccumulator(initExpressions, initBlock, accPhysType);

        final Expression accumulatorInitializer =
                builder.append("accumulatorInitializer",
                        Expressions.lambda(
                                Function0.class,
                                initBlock.toBlock()));

        // Function2<Object[], Employee, Object[]> accumulatorAdder =
        //     new Function2<Object[], Employee, Object[]>() {
        //         public Object[] apply(Object[] acc, Employee in) {
        //              acc[0] = ((Integer) acc[0]) + 1;
        //              acc[1] = ((Integer) acc[1]) + in.salary;
        //             return acc;
        //         }
        //     };
        final ParameterExpression inParameter =
                Expressions.parameter(inputPhysType.getJavaRowType(), "in");
        final ParameterExpression acc_ =
                Expressions.parameter(accPhysType.getJavaRowType(), "acc");

        createAccumulatorAdders(
                inParameter, aggs, accPhysType, acc_, inputPhysType, builder, implementor, typeFactory);

        final ParameterExpression lambdaFactory =
                Expressions.parameter(AggregateLambdaFactory.class,
                        builder.newName("lambdaFactory"));

        implementLambdaFactory(builder, inputPhysType, aggs, accumulatorInitializer,
                hasOrderedCall(aggs), lambdaFactory);
        // Function2<Integer, Object[], Object[]> resultSelector =
        //     new Function2<Integer, Object[], Object[]>() {
        //         public Object[] apply(Integer key, Object[] acc) {
        //             return new Object[] { key, acc[0], acc[1] };
        //         }
        //     };
        final BlockBuilder resultBlock = new BlockBuilder();
        final List<Expression> results = Expressions.list();
        final ParameterExpression key_;
        if (groupCount == 0) {
            key_ = null;
        } else {
            final Type keyType = keyPhysType.getJavaRowType();
            key_ = Expressions.parameter(keyType, "key");
            for (int j = 0; j < groupCount; j++) {
                final Expression ref = keyPhysType.fieldReference(key_, j);
                if (getGroupType() == Group.SIMPLE) {
                    results.add(ref);
                } else {
                    results.add(
                            Expressions.condition(
                                    keyPhysType.fieldReference(key_, groupCount + j),
                                    Expressions.constant(null),
                                    Expressions.box(ref)));
                }
            }
        }
        for (final AggImpState agg : aggs) {
            results.add(
                    agg.implementor.implementResult(agg.context,
                            new AggResultContextImpl(resultBlock, agg.call, agg.state, key_,
                                    keyPhysType)));
        }
        resultBlock.add(physType.record(results));
        if (getGroupType() != Group.SIMPLE) {
            final List<Expression> list = new ArrayList<>();
            for (ImmutableBitSet set : groupSets) {
                list.add(
                        inputPhysType.generateSelector(parameter, groupSet.asList(),
                                set.asList(), keyPhysType.getFormat()));
            }
            final Expression keySelectors_ =
                    builder.append("keySelectors",
                            Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
                                    list));
            final Expression resultSelector =
                    builder.append("resultSelector",
                            Expressions.lambda(Function2.class,
                                    resultBlock.toBlock(),
                                    key_,
                                    acc_));
            builder.add(
                    Expressions.return_(null,
                            Expressions.call(
                                    BuiltInMethod.GROUP_BY_MULTIPLE.method,
                                    Expressions.list(childExp,
                                            keySelectors_,
                                            Expressions.call(lambdaFactory,
                                                    BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method),
                                            Expressions.call(lambdaFactory,
                                                    BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method),
                                            Expressions.call(lambdaFactory,
                                                    BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR.method,
                                                    resultSelector))
                                            .appendIfNotNull(keyPhysType.comparer()))));
        } else if (groupCount == 0) {
            final Expression resultSelector =
                    builder.append(
                            "resultSelector",
                            Expressions.lambda(
                                    Function1.class,
                                    resultBlock.toBlock(),
                                    acc_));
            builder.add(
                    Expressions.return_(
                            null,
                            Expressions.call(
                                    BuiltInMethod.SINGLETON_ENUMERABLE.method,
                                    Expressions.call(
                                            childExp,
                                            BuiltInMethod.AGGREGATE.method,
                                            Expressions.call(
                                                    Expressions.call(lambdaFactory,
                                                            BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method),
                                                    BuiltInMethod.FUNCTION0_APPLY.method),
                                            Expressions.call(lambdaFactory,
                                                    BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method),
                                            Expressions.call(lambdaFactory,
                                                    BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_SINGLE_GROUP_RESULT_SELECTOR.method,
                                                    resultSelector)))));
        } else if (aggCalls.isEmpty()
                && groupSet.equals(
                ImmutableBitSet.range(child.getRowType().getFieldCount()))) {
            builder.add(
                    Expressions.return_(
                            null,
                            Expressions.call(
                                    inputPhysType.convertTo(childExp, physType.getFormat()),
                                    BuiltInMethod.DISTINCT.method,
                                    Expressions.<Expression>list()
                                            .appendIfNotNull(physType.comparer()))));
        } else {
            final Expression keySelector_ =
                    builder.append("keySelector",
                            inputPhysType.generateSelector(parameter,
                                    groupSet.asList(),
                                    keyPhysType.getFormat()));
            final Expression resultSelector_ =
                    builder.append("resultSelector",
                            Expressions.lambda(Function2.class,
                                    resultBlock.toBlock(),
                                    key_,
                                    acc_));
            builder.add(
                    Expressions.return_(null,
                            Expressions.call(childExp,
                                    BuiltInMethod.GROUP_BY2.method,
                                    Expressions.list(keySelector_,
                                            Expressions.call(lambdaFactory,
                                                    BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method),
                                            Expressions.call(lambdaFactory,
                                                    BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method),
                                            Expressions.call(lambdaFactory,
                                                    BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR.method,
                                                    resultSelector_))
                                            .appendIfNotNull(keyPhysType.comparer()))));
        }
        return implementor.result(physType, builder.toBlock());
    }

}
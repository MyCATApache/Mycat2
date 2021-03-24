/**
 * Copyright (C) <2021>  <chen junwen>
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
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class MycatSortAgg extends EnumerableAggregateBase implements MycatRel {

    protected MycatSortAgg(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                           ImmutableBitSet groupSet,
                           List<ImmutableBitSet> groupSets,
                           List<AggregateCall> aggCalls) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    }

    public static MycatSortAgg create(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        RelOptCluster cluster = input.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        traitSet= traitSet.replace(MycatConvention.INSTANCE);
        traitSet=traitSet .replaceIfs(
                RelCollationTraitDef.INSTANCE,
                () -> mq.collations(input));//sortagg结果也是已经排序的
        return new MycatSortAgg(cluster,traitSet,input,groupSet,groupSets,aggCalls);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("SortAgg");

        ((MycatRel) input).explain(writer);
        return writer.ret();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new MycatSortAgg(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        if (!Aggregate.isSimple(this)) {
            throw Util.needToImplement("EnumerableSortedAggregate");
        }

        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableRel child = (EnumerableRel) getInput();
        final Result result = implementor.visitChild(this, 0, child, pref);
        Expression childExp =
                toEnumerate(builder.append(
                        "child",
                        result.block));

        final PhysType physType =
                PhysTypeImpl.of(
                        typeFactory, getRowType(), pref.preferCustom());

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
                false, lambdaFactory);

        final BlockBuilder resultBlock = new BlockBuilder();
        final List<Expression> results = Expressions.list();
        final ParameterExpression key_;
        final Type keyType = keyPhysType.getJavaRowType();
        key_ = Expressions.parameter(keyType, "key");
        for (int j = 0; j < groupCount; j++) {
            final Expression ref = keyPhysType.fieldReference(key_, j);
            results.add(ref);
        }

        for (final AggImpState agg : aggs) {
            results.add(
                    agg.implementor.implementResult(agg.context,
                            new AggResultContextImpl(resultBlock, agg.call, agg.state, key_,
                                    keyPhysType)));
        }
        resultBlock.add(physType.record(results));

        final Expression keySelector_ =
                builder.append("keySelector",
                        inputPhysType.generateSelector(parameter,
                                groupSet.asList(),
                                keyPhysType.getFormat()));
        // Generate the appropriate key Comparator. In the case of NULL values
        // in group keys, the comparator must be able to support NULL values by giving a
        // consistent sort ordering.
        final Expression comparator = keyPhysType.generateComparator(getTraitSet().getCollation());

        final Expression resultSelector_ =
                builder.append("resultSelector",
                        Expressions.lambda(Function2.class,
                                resultBlock.toBlock(),
                                key_,
                                acc_));

        builder.add(
                Expressions.return_(null,
                        Expressions.call(childExp,
                                BuiltInMethod.SORTED_GROUP_BY.method,
                                Expressions.list(keySelector_,
                                        Expressions.call(lambdaFactory,
                                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method),
                                        Expressions.call(lambdaFactory,
                                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method),
                                        Expressions.call(lambdaFactory,
                                                BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR.method,
                                                resultSelector_), comparator)
                        )));

        return implementor.result(physType, builder.toBlock());
    }
    @Override
    public boolean isSupportStream() {
        return true;
    }
}
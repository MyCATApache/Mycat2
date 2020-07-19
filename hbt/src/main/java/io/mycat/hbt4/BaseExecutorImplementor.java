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
package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt3.PartInfo;
import io.mycat.hbt3.View;
import io.mycat.hbt4.executor.*;
import io.mycat.hbt4.logical.*;
import io.mycat.hbt4.physical.*;
import io.mycat.mpp.Row;
import lombok.SneakyThrows;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.NotNull;
import org.objenesis.instantiator.util.UnsafeUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

public abstract class BaseExecutorImplementor implements ExecutorImplementor {
    final List<Object> context;
    private TempResultSetFactory tempResultSetFactory;

    public BaseExecutorImplementor(List<Object> context, TempResultSetFactory tempResultSetFactory) {
        this.context = context;
        this.tempResultSetFactory = tempResultSetFactory;
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatNestedLoopJoin mycatJoin) {
        RexNode condition = mycatJoin.getCondition();
        JoinInfo joinInfo = mycatJoin.analyzeCondition();
        int leftFieldCount = mycatJoin.getLeft().getRowType().getFieldCount();
        int rightFieldCount = mycatJoin.getRight().getRowType().getFieldCount();
        Executor[] executors = implementInputs(mycatJoin);
        Executor leftSource = executors[0];
        Executor rightSource = executors[1];
        JaninoRexCompiler compiler = new JaninoRexCompiler(MycatCalciteSupport.INSTANCE.RexBuilder);
        Scalar scalar = compiler.compile(ImmutableList.of(
                condition),
                combinedRowType(mycatJoin.getInputs())
        );
        Context o = (Context) UnsafeUtils.getUnsafe().allocateInstance(Context.class);
        final Function2<Row, Row, Row> resultSelector = Row.composeJoinRow(leftFieldCount, rightFieldCount);
        Predicate2<Row, Row> predicate = (v0, v1) -> {
            o.values = resultSelector.apply(v0, v1).values;
            return scalar.execute(o) == Boolean.TRUE;
        };
        return new MycatNestedLoopJoinExecutor(mycatJoin.getJoinType(), leftSource, rightSource, resultSelector, predicate, tempResultSetFactory);
    }


    @Override
    public Executor implement(MycatCalc mycatCalc) {
        return null;
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatProject mycatProject) {
        Executor[] executors = implementInputs(mycatProject);
        RelDataType inputRowType = mycatProject.getInput().getRowType();
        List<RexNode> childExps = mycatProject.getChildExps();
        int outputSize = childExps.size();
        JaninoRexCompiler compiler = new JaninoRexCompiler(MycatCalciteSupport.INSTANCE.RexBuilder);
        Scalar scalar = compiler.compile(childExps, inputRowType);
        Context o = (Context) UnsafeUtils.getUnsafe().allocateInstance(Context.class);
        MycatScalar mycatScalar = (input, output) -> {
            o.values = input.values;
            Object[] outputValues = new Object[outputSize];
            scalar.execute(o, outputValues);
            output.values = outputValues;
        };
        return new MycatProjectExecutor(mycatScalar, executors[0]);
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatFilter mycatFilter) {
        Executor input = implementInput(mycatFilter);
        RelDataType inputRowType = mycatFilter.getInput().getRowType();
        RexNode condition = mycatFilter.getCondition();
        JaninoRexCompiler compiler = new JaninoRexCompiler(MycatCalciteSupport.INSTANCE.RexBuilder);
        Scalar scalar = compiler.compile(ImmutableList.of(condition), inputRowType);
        Context o = (Context) UnsafeUtils.getUnsafe().allocateInstance(Context.class);
        Predicate<Row> predicate = row -> {
            o.values = row.values;
            return scalar.execute(o) == Boolean.TRUE;
        };
        return new MycatFilterExecutor(predicate, input);
    }

    @Override
    public Executor implement(MycatAggregate mycatAggregate) {
        Executor input = implementInput(mycatAggregate);
        return new MycatHashAggExecutor(input, mycatAggregate);
    }

    @Override
    public Executor implement(MycatUnion mycatUnion) {
        Executor[] executors = implementInputs(mycatUnion);
        if (mycatUnion.all) return new MycatUnionAllExecutor(executors);
        else return new MycatUnionDistinctExecutor(executors);
    }

    @Override
    public Executor implement(MycatIntersect mycatIntersect) {
        Executor[] executors = implementInputs(mycatIntersect);
        return new MycatIntersectExecutor(executors, mycatIntersect.all);
    }

    @Override
    public Executor implement(MycatMinus mycatMinus) {
        Executor[] executors = implementInputs(mycatMinus);
        return new MycatMinusExecutor(executors, mycatMinus.all);
    }

    @Override
    public Executor implement(MycatTableModify mycatTableModify) {
        return null;
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatValues mycatValues) {
        final List<RexNode> nodes = new ArrayList<>();
        for (ImmutableList<RexLiteral> tuple : mycatValues.tuples) {
            nodes.addAll(tuple);
        }
        int fieldCount = mycatValues.getRowType().getFieldCount();
        JaninoRexCompiler compiler = new JaninoRexCompiler(MycatCalciteSupport.INSTANCE.RexBuilder);
        final Scalar scalar = compiler.compile(nodes, MycatCalciteSupport.INSTANCE
                .TypeFactory.builder().build());
        final Object[] values = new Object[nodes.size()];
        Context context = (Context) UnsafeUtils.getUnsafe().allocateInstance(Context.class);
        scalar.execute(context, values);
        final ImmutableList.Builder<Row> rows = ImmutableList.builder();


        for (int i = 0; i < values.length; i += fieldCount) {
            Object[] r = new Object[fieldCount];
            for (int j = i, k = 0; k < fieldCount; j++, k++) {
                r[k] = values[j];
                rows.add(Row.of(r));
            }
        }
        return new MycatValuesExecutor(rows.build());
    }

    @Override
    public Executor implement(MycatSort mycatSort) {
        return createSort(mycatSort, false);
    }

    @NotNull
    public Executor createSort(Sort mycatSort, boolean mergeSort) {
        RelCollation collation = mycatSort.getCollation();
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        RexNode offset = mycatSort.offset;
        RexNode fetch = mycatSort.fetch;
        Comparator<Row> comparator = null;
        long offsetValue = 0;
        long fetchValue = 0;
        if (!fieldCollations.isEmpty()) {
            comparator = comparator(mycatSort);
        }

        if (offset != null || fetch != null) {
            offset = resolveDynamicParam(offset);
            fetch = resolveDynamicParam(fetch);
            offsetValue =
                    offset == null
                            ? 0
                            : ((RexLiteral) offset).getValueAs(Long.class);

            fetchValue = fetch == null
                    ? Long.MAX_VALUE
                    : ((RexLiteral) fetch).getValueAs(Long.class);
        }
        if (mergeSort) {
            Executor[] executors = implementInputs(mycatSort);
            MycatMergeSortExecutor mycatMergeSortExecutor = new MycatMergeSortExecutor(comparator, executors);
            if ((offset != null || fetch != null)) {
                return new MycatLimitExecutor(offsetValue, fetchValue, mycatMergeSortExecutor);
            } else {
                return mycatMergeSortExecutor;
            }
        } else {
            Executor executor = implementInput((MycatRel) mycatSort);
            boolean isTopN = comparator != null && (offset != null || fetch != null);
            if (isTopN) {
                return new MycatTopNExecutor(comparator, offsetValue, fetchValue, executor);
            }
            if (comparator != null) {
                return new MycatMemSortExecutor(comparator, executor);
            }
            return new MycatLimitExecutor(offsetValue, fetchValue, executor);
        }
    }

    private RexNode resolveDynamicParam(RexNode node) {
        if (node == null) {
            return null;
        }
        if (node instanceof RexDynamicParam) {
            RelDataType type = node.getType();
            Object o = context.get(((RexDynamicParam) node).getIndex());
            return MycatCalciteSupport.INSTANCE.RexBuilder.makeLiteral(o, type, true);
        }
        return node;
    }

    @Override
    public Executor implement(QueryView gatherView) {
        return null;
    }


    @Override
    public Executor implement(HashAgg hashAgg) {
        return null;
    }

    @Override
    public Executor implement(HashJoin hashJoin) {
        return null;
    }

    @Override
    public Executor implement(MaterializedSemiJoin materializedSemiJoin) {
        return null;
    }

    @Override
    public Executor implement(MemSort memSort) {
        return null;
    }

    @Override
    public Executor implement(MycatMergeSort mergeSort) {
        return createSort(mergeSort, true);
    }

    @Override
    public Executor implement(NestedLoopJoin nestedLoopJoin) {
//        RexNode condition = nestedLoopJoin.getCondition();
//        RelNode leftExecutor = implementInput(nestedLoopJoin.getLeft());
//        RelNode right = nestedLoopJoin.getRight();

        return null;
    }

    @Override
    public Executor implement(SemiHashJoin semiHashJoin) {
        return null;
    }

    @Override
    public Executor implement(SortAgg sortAgg) {
        return null;
    }

    @Override
    public Executor implement(MycatSortMergeJoin sortMergeJoin) {
        Executor[] executors = implementInputs(sortMergeJoin);
        JoinRelType joinType = sortMergeJoin.getJoinType();

        JoinInfo joinInfo = sortMergeJoin.analyzeCondition();
        ImmutableList<RexNode> nonEquiConditions = joinInfo.nonEquiConditions;//不等价条件

        int[] leftKeys = joinInfo.leftKeys.toIntArray();
        int[] rightKeys = joinInfo.leftKeys.toIntArray();
        int leftFieldCount = sortMergeJoin.getLeft().getRowType().getFieldCount();
        int rightFieldCount = sortMergeJoin.getRight().getRowType().getFieldCount();
        RelDataType resultRelDataType = combinedRowType(sortMergeJoin.getInputs());
        return new MycatMergeJoinExecutor(sortMergeJoin, joinType, executors[0], executors[1]
                , nonEquiConditions, leftKeys, rightKeys, leftFieldCount, rightFieldCount, resultRelDataType);
    }

    @Override
    public Executor implement(SortMergeSemiJoin sortMergeSemiJoin) {
        return null;
    }

    @Override
    public Executor implement(TopN topN) {
        return null;
    }

    @Override
    public Executor implement(MycatQuery mycatQuery) {
        View view = mycatQuery.getView();
        PartInfo dataNode = view.getDataNode();
        String sql = view.getSql();
        return new ScanExecutor();
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatHashJoin mycatHashJoin) {
        Executor[] executors = implementInputs(mycatHashJoin);
        JoinRelType joinType = mycatHashJoin.getJoinType();

        JoinInfo joinInfo = mycatHashJoin.analyzeCondition();
        ImmutableList<RexNode> nonEquiConditions = joinInfo.nonEquiConditions;//不等价条件

        int[] leftKeys = joinInfo.leftKeys.toIntArray();
        int[] rightKeys = joinInfo.leftKeys.toIntArray();
        boolean generateNullsOnLeft = mycatHashJoin.getJoinType().generatesNullsOnLeft();
        boolean generateNullsOnRight = mycatHashJoin.getJoinType().generatesNullsOnRight();
        int leftFieldCount = mycatHashJoin.getLeft().getRowType().getFieldCount();
        int rightFieldCount = mycatHashJoin.getRight().getRowType().getFieldCount();
        RelDataType resultRelDataType = combinedRowType(mycatHashJoin.getInputs());
        return new MycatHashJoinExecutor(mycatHashJoin, joinType,
                executors[0],
                executors[1],
                nonEquiConditions,
                leftKeys,
                rightKeys,
                generateNullsOnLeft,
                generateNullsOnRight,
                leftFieldCount,
                rightFieldCount,
                resultRelDataType, tempResultSetFactory);
    }


    private Executor implementInput(MycatRel rel) {
        return implementInputs(rel)[0];
    }

    private Executor[] implementInputs(RelNode rel) {
        List<RelNode> inputs = rel.getInputs();
        int size = inputs.size();
        Executor[] executors = new Executor[size];
        for (int i = 0; i < size; i++) {
            RelNode input = inputs.get(i);
            if (input instanceof MycatRel) {
                executors[i] = ((MycatRel) input).implement(this);
            } else {
                throw new UnsupportedOperationException();
            }
        }
        return executors;
    }

    private Comparator<Row> comparator(Sort rel) {
        if (rel.getCollation().getFieldCollations().size() == 1) {
            return comparator(rel.getCollation().getFieldCollations().get(0));
        }
        return Ordering.compound(
                Iterables.transform(rel.getCollation().getFieldCollations(),
                        this::comparator));
    }

    private Comparator<Row> comparator(RelFieldCollation fieldCollation) {
        final int nullComparison = fieldCollation.nullDirection.nullComparison;
        final int x = fieldCollation.getFieldIndex();
        switch (fieldCollation.direction) {
            case ASCENDING:
                return (o1, o2) -> {
                    final Comparable c1 = (Comparable) o1.getValues()[x];
                    final Comparable c2 = (Comparable) o2.getValues()[x];
                    return RelFieldCollation.compare(c1, c2, nullComparison);
                };
            default:
                return (o1, o2) -> {
                    final Comparable c1 = (Comparable) o1.getValues()[x];
                    final Comparable c2 = (Comparable) o2.getValues()[x];
                    return RelFieldCollation.compare(c2, c1, -nullComparison);
                };
        }
    }

    public RelDataType combinedRowType(List<RelNode> inputs) {
        final RelDataTypeFactory.Builder builder = MycatCalciteSupport.INSTANCE.TypeFactory.builder();
        for (RelNode input : inputs) {
            builder.addAll(input.getRowType().getFieldList());
        }
        return builder.build();
    }
}
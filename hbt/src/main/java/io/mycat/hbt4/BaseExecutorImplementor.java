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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt3.MycatLookUpView;
import io.mycat.hbt3.Distribution;
import io.mycat.hbt3.View;
import io.mycat.hbt4.executor.*;
import io.mycat.hbt4.logical.rel.*;
import io.mycat.mpp.Row;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Predicate;

public abstract class BaseExecutorImplementor implements ExecutorImplementor {
    final static Logger log = LoggerFactory.getLogger(BaseExecutorImplementor.class);

    final MycatContext context;
    final Map<String, RexToLixTranslator.InputGetter> ref = new HashMap<>();
    final Map<String, Cor[]> refValue = new HashMap<>();
    private TempResultSetFactory tempResultSetFactory;

    boolean isCorrelate() {
        return !ref.isEmpty();
    }

    public BaseExecutorImplementor(MycatContext context, TempResultSetFactory tempResultSetFactory) {
        this.context = context;
        this.tempResultSetFactory = tempResultSetFactory;
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatNestedLoopJoin mycatJoin) {
        return createNestedLoopJoin(mycatJoin);
    }

    @NotNull
    public Executor createNestedLoopJoin(Join mycatJoin) {
        RexNode condition = mycatJoin.getCondition();
        int leftFieldCount = mycatJoin.getLeft().getRowType().getFieldCount();
        int rightFieldCount = mycatJoin.getRight().getRowType().getFieldCount();
        Executor[] executors = implementInputs(mycatJoin);
        Executor leftSource = executors[0];
        Executor rightSource = executors[1];
        MycatScalar scalar = MycatRexCompiler.compile(ImmutableList.of(
                condition),
                combinedRowType(mycatJoin.getInputs())
        );
        log.info("-------------------complie----------------");
        final Function2<Row, Row, Row> resultSelector = Row.composeJoinRow(leftFieldCount, rightFieldCount);
        MycatContext context = new MycatContext();
        Predicate2<Row, Row> predicate = (v0, v1) -> {
            context.values = resultSelector.apply(v0, v1).values;
            return scalar.execute(context) == Boolean.TRUE;
        };
        return MycatNestedLoopJoinExecutor.create(mycatJoin.getJoinType(), leftSource, rightSource, resultSelector, predicate, tempResultSetFactory);
    }


    @Override
    public Executor implement(MycatCalc mycatCalc) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatProject mycatProject) {
        Executor executor = implementInput(mycatProject);
//        if (isCorrelate() && (!executor.isRewindSupported())) {
//            executor = tempResultSetFactory.makeRewind(executor);
//        }
        RelDataType inputRowType = mycatProject.getInput().getRowType();
        List<RexNode> childExps = mycatProject.getChildExps();
        int outputSize = childExps.size();
        log.info("-------------------complie----------------");
        MycatScalar scalar = MycatRexCompiler.compile(childExps, inputRowType, this::refInput);
        return MycatProjectExecutor.create((input) -> {
            context.values = input.values;
            Object[] outputValues = new Object[outputSize];
            scalar.execute(context, outputValues);
            return Row.of(outputValues);
        }, executor);
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatFilter mycatFilter) {
        Executor input = implementInput(mycatFilter);

//        if (isCorrelate() && (!input.isRewindSupported())) {
//            input = tempResultSetFactory.makeRewind(input);
//        }
        RelDataType inputRowType = mycatFilter.getInput().getRowType();
        ImmutableList<RexNode> conditions = ImmutableList.of(mycatFilter.getCondition());
        log.info("-------------------complie----------------");
        MycatScalar scalar = MycatRexCompiler.compile(conditions, inputRowType, this::refInput);
        Predicate<Row> predicate = row -> {
            context.values = row.values;
            return scalar.execute(context) == Boolean.TRUE;
        };
        return MycatFilterExecutor.create(predicate, input);
    }

    public RexToLixTranslator.InputGetter refInput(String name) {
        return ref.get(name);
    }

    @Override
    public Executor implement(MycatHashAggregate mycatAggregate) {
        Executor input = implementInput(mycatAggregate);
        return MycatHashAggExecutor.create(input, mycatAggregate);
    }

    @Override
    public Executor implement(MycatUnion mycatUnion) {
        Executor[] executors = implementInputs(mycatUnion);
        if (mycatUnion.all) return MycatUnionAllExecutor.create(executors);
        else return MycatUnionDistinctExecutor.create(executors);
    }

    @Override
    public Executor implement(MycatIntersect mycatIntersect) {
        Executor[] executors = implementInputs(mycatIntersect);
        return MycatIntersectExecutor.create(executors, mycatIntersect.all);
    }

    @Override
    public Executor implement(MycatMinus mycatMinus) {
        Executor[] executors = implementInputs(mycatMinus);
        return MycatMinusExecutor.create(executors, mycatMinus.all);
    }

    @Override
    public Executor implement(MycatTableModify mycatTableModify) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatValues mycatValues) {
        final List<RexNode> nodes = new ArrayList<>();
        for (ImmutableList<RexLiteral> tuple : mycatValues.tuples) {
            nodes.addAll(tuple);
        }
        int fieldCount = mycatValues.getRowType().getFieldCount();
        final MycatScalar scalar = MycatRexCompiler.compile(nodes, null);
        final Object[] values = new Object[nodes.size()];
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
    public Executor implement(MycatMemSort mycatSort) {
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
                return MycatLimitExecutor.create(offsetValue, fetchValue, mycatMergeSortExecutor);
            } else {
                return mycatMergeSortExecutor;
            }
        } else {
            Executor executor = implementInput((MycatRel) mycatSort);
            boolean isTopN = comparator != null && (offset != null || fetch != null);
            if (isTopN) {
                return MycatTopNExecutor.create(comparator, offsetValue, fetchValue, executor);
            }
            if (comparator != null) {
                return MycatMemSortExecutor.create(comparator, executor);
            }
            return MycatLimitExecutor.create(offsetValue, fetchValue, executor);
        }
    }

    private RexNode resolveDynamicParam(RexNode node) {
        if (node == null) {
            return null;
        }
        if (node instanceof RexDynamicParam) {
            RelDataType type = node.getType();
//            Object o = context.get(((RexDynamicParam) node).getIndex());
//            return MycatCalciteSupport.INSTANCE.RexBuilder.makeLiteral(o, type, true);
            throw new UnsupportedOperationException();
        }
        return node;
    }

    @Override
    public Executor implement(QueryView gatherView) {
        return null;
    }

    @Override
    public Executor implement(MycatMaterializedSemiJoin materializedSemiJoin) {
        return createNestedLoopJoin(materializedSemiJoin);
    }

    @Override
    public Executor implement(MycatMergeSort mergeSort) {
        return createSort(mergeSort, true);
    }

    @Override
    public Executor implement(MycatSemiHashJoin semiHashJoin) {
        return createHashJoin(semiHashJoin);
    }

    @Override
    public Executor implement(MycatSortAgg sortAgg) {
        Executor executor = implementInput(sortAgg);
        return MycatSortAggExecutor.create(executor, sortAgg);
    }

    @Override
    public Executor implement(MycatSortMergeJoin sortMergeJoin) {
        return createSortMergeJoin(sortMergeJoin);
    }

    @NotNull
    public Executor createSortMergeJoin(Join sortMergeJoin) {
        Executor[] executors = implementInputs(sortMergeJoin);
        JoinRelType joinType = sortMergeJoin.getJoinType();

        JoinInfo joinInfo = sortMergeJoin.analyzeCondition();
        ImmutableList<RexNode> nonEquiConditions = joinInfo.nonEquiConditions;//不等价条件

        int[] leftKeys = joinInfo.leftKeys.toIntArray();
        int[] rightKeys = joinInfo.leftKeys.toIntArray();
        int leftFieldCount = sortMergeJoin.getLeft().getRowType().getFieldCount();
        int rightFieldCount = sortMergeJoin.getRight().getRowType().getFieldCount();
        RelDataType resultRelDataType = combinedRowType(sortMergeJoin.getInputs());
        return MycatMergeJoinExecutor.create(
                joinType,
                executors[0],
                executors[1],
                nonEquiConditions,
                leftKeys,
                rightKeys,
                leftFieldCount,
                rightFieldCount,
                resultRelDataType);
    }

    @Override
    public Executor implement(MycatSortMergeSemiJoin sortMergeSemiJoin) {
        return createSortMergeJoin(sortMergeSemiJoin);
    }

    @Override
    public Executor implement(MycatTopN topN) {
        return createSort(topN, false);
    }

    @Override
    public Executor implement(MycatQuery mycatQuery) {
        View view = mycatQuery.getView();
        Distribution dataNode = view.getDataNode();
        String sql = view.getSql();
        return ScanExecutor.createDemo();
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatHashJoin mycatHashJoin) {
        return createHashJoin(mycatHashJoin);
    }

    @NotNull
    public Executor createHashJoin(Join mycatHashJoin) {
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
        return new MycatHashJoinExecutor(joinType,
                executors[0],
                executors[1],
                nonEquiConditions,
                leftKeys,
                rightKeys,
                generateNullsOnLeft,
                generateNullsOnRight,
                leftFieldCount,
                rightFieldCount,
                resultRelDataType);
    }

    public static class Cor {
        RelDataType type;
        Object value;
    }

    public static class Level {
        String name;
        ;
    }

    @Override
    public Executor implement(MycatCorrelate mycatCorrelate) {
        JoinRelType joinType = mycatCorrelate.getJoinType();
        String correlVariable = mycatCorrelate.getCorrelVariable();
        int[] requiredColumns = mycatCorrelate.getRequiredColumns().toArray();
        MycatRel left = (MycatRel) mycatCorrelate.getLeft();
        MycatRel right = (MycatRel) mycatCorrelate.getRight();
        int leftFieldCount = left.getRowType().getFieldCount();
        List<RelDataTypeField> fieldList = left.getRowType().getFieldList();
        int rightFieldCount = right.getRowType().getFieldCount();
        Executor leftExecutor = implementInput(left);
        JavaTypeFactoryImpl typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;

//        this.ref.computeIfAbsent(correlVariable, s -> a0 -> new DataContextInputGetter(left.getRowType(), typeFactory));
//
        Executor rightExecutor = implementInput(right);
        this.ref.remove(correlVariable);

        Enumerable<Row> leftEnumerable = Linq4j.asEnumerable(Linq4j.asEnumerable(leftExecutor));
        Cor[] cors = this.refValue.computeIfAbsent(correlVariable, (Function<String, Cor[]>) input -> new Cor[requiredColumns.length]);
        final Function1<Row, Enumerable<Row>> inner = a0 -> {
            int index = 0;
            for (int requiredColumn : requiredColumns) {
                Cor curCor = cors[index];
                curCor.value = a0.getObject(requiredColumn);
                index++;
            }
            rightExecutor.open();
            return Linq4j.asEnumerable(rightExecutor);
        };
        final Function2<Row, Row, Row> resultSelector = (v0, v1) -> {
            if (v0 == null) {
                v0 = Row.create(leftFieldCount);
            }
            if (v1 == null) {
                v1 = Row.create(rightFieldCount);
            }
            return v0.compose(v1);
        };
        return MycatCorrelateExecutor.create(EnumerableDefaults.correlateJoin(JoinType.valueOf(joinType.name()), leftEnumerable, inner, resultSelector));

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

    public static Comparator<Row> comparator(Sort rel) {
        List<RelFieldCollation> fieldCollations = rel.getCollation().getFieldCollations();
        return comparator(fieldCollations);
    }

    @NotNull
    public static Comparator<Row> comparator(List<RelFieldCollation> fieldCollations) {
        if (fieldCollations.size() == 1) return comparator(fieldCollations.get(0));
        return Ordering.compound(
                Iterables.transform(fieldCollations, new Function<RelFieldCollation, Comparator<? super Row>>() {
                    @Nullable
                    @Override
                    public Comparator<? super Row> apply(@Nullable RelFieldCollation input) {
                        return comparator(input);
                    }
                }));
    }

    public static Comparator<Row> comparator(RelFieldCollation fieldCollation) {
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

    private static class DataContextInputGetter implements RexToLixTranslator.InputGetter {
        private final String name;
        private final RelDataTypeFactory typeFactory;
        private final RelDataType rowType;

        DataContextInputGetter(String name, RelDataType rowType,
                               RelDataTypeFactory typeFactory) {
            this.name = name;
            this.rowType = rowType;
            this.typeFactory = typeFactory;
        }

        public Expression field(BlockBuilder list, int index, Type storageType) {
            MethodCallExpression recFromCtx = Expressions.call(MycatBuiltInMethod.ROOT, "getSlots");
            Expression recFromCtxCasted =
                    EnumUtils.convert(recFromCtx, Object[].class);
            IndexExpression recordAccess = Expressions.arrayIndex(recFromCtxCasted,
                    Expressions.constant(index));
            if (storageType == null) {
                final RelDataType fieldType =
                        rowType.getFieldList().get(index).getType();
                storageType = ((JavaTypeFactory) typeFactory).getJavaClass(fieldType);
            }
            return EnumUtils.convert(recordAccess, storageType);
        }

        public String getName() {
            return name;
        }
    }

    @Override
    public Executor implement(MycatBatchNestedLoopJoin mycatBatchNestedLoopJoin) {
        Set<CorrelationId> variablesSet = mycatBatchNestedLoopJoin.getVariablesSet();
        ImmutableBitSet requiredColumns = mycatBatchNestedLoopJoin.getRequiredColumns();
        Function1<Row, Row> projectJoinKey = createProjectJoinKeys(requiredColumns);
        System.out.println(projectJoinKey);
//        variablesSet.forEach(v-> {
//            String name = v.getName();
//            ref.put(name, new DataContextInputGetter(
//                    name,
//                    mycatBatchNestedLoopJoin.getRight().getRowType(),
//                    MycatCalciteSupport.INSTANCE.TypeFactory
//            ));
//        });
        try {
            Executor[] executors = implementInputs(mycatBatchNestedLoopJoin);
            JoinRelType joinType = mycatBatchNestedLoopJoin.getJoinType();
            int leftFieldCount = mycatBatchNestedLoopJoin.getLeft().getRowType().getFieldCount();
            int rightFieldCount = mycatBatchNestedLoopJoin.getRight().getRowType().getFieldCount();

            Executor leftSource = executors[0];
            MycatLookupExecutor rightSource = (MycatLookupExecutor) executors[1];

            MycatScalar scalar = MycatRexCompiler.compile(ImmutableList.of(
                    mycatBatchNestedLoopJoin.getCondition()),
                    combinedRowType(mycatBatchNestedLoopJoin.getInputs())
            );
            final Function2<Row, Row, Row> resultSelector = Row.composeJoinRow(leftFieldCount, rightFieldCount);
            MycatContext context = new MycatContext();
            Predicate2<Row, Row> predicate = (v0, v1) -> {
                context.values = resultSelector.apply(v0, v1).values;
                return scalar.execute(context) == Boolean.TRUE;
            };
            TempResultSetFactory tempResultSetFactory = this.tempResultSetFactory;

            return MycatBatchNestedLoopJoinExecutor.create(
                    JoinType.valueOf(joinType.name()),
                    leftSource,
                    rightSource,
                    leftFieldCount,
                    rightFieldCount,
                    predicate,
                    predicate
            );
        } finally {
//            variablesSet.forEach(n->ref.remove(n.getName()));
        }
    }

    @NotNull
    public Function1<Row, Row> createProjectJoinKeys(ImmutableBitSet requiredColumns) {
        int[] ints = requiredColumns.toArray();
        return a0 -> {
            Row res = Row.create(ints.length);
            int index = 0;
            for (int projectIndex : ints) {
                res.set(index, a0.getObject(projectIndex));
                index++;
            }
            return res;
        };
    }

    @Override
    public Executor implement(MycatLookUpView mycatLookUpView) {
        return MycatLookupExecutor.create(mycatLookUpView.getRelNode());
    }

    @Override
    public Executor implement(MycatGather mycatGather) {
        Executor[] executors = implementInputs(mycatGather);
        return MycatGatherExecutor.create(Arrays.asList(executors));
    }
}
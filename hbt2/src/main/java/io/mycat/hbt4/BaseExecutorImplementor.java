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
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.objenesis.instantiator.util.UnsafeUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

public abstract class BaseExecutorImplementor implements ExecutorImplementor {
    final List<Object> context;

    public BaseExecutorImplementor(List< Object> context) {
        this.context = context;
    }

    @Override
    @SneakyThrows
    public Executor implement(MycatJoin mycatJoin) {

        JoinInfo joinInfo = mycatJoin.analyzeCondition();
        ImmutableList<RexNode> nonEquiConditions = joinInfo.nonEquiConditions;//不等价条件

        int[] leftKeys = joinInfo.leftKeys.toIntArray();
        int[] rightKeys = joinInfo.leftKeys.toIntArray();
        boolean generateNullsOnLeft = mycatJoin.getJoinType().generatesNullsOnLeft();
        boolean generateNullsOnRight = mycatJoin.getJoinType().generatesNullsOnRight();


        Executor[] executors = implementInputs(mycatJoin);


        //开始编译
        Executor leftSource = executors[0];
        Executor rightSource = executors[1];

        JaninoRexCompiler compiler = new JaninoRexCompiler(MycatCalciteSupport.INSTANCE.RexBuilder);
        Scalar scalar = compiler.compile(ImmutableList.of(
                RexUtil.composeConjunction(MycatCalciteSupport.INSTANCE.RexBuilder, nonEquiConditions)),
                combinedRowType(mycatJoin.getInputs())
        );
        Context o = (Context) UnsafeUtils.getUnsafe().allocateInstance(Context.class);

        final Enumerable<Row> outer = Linq4j.asEnumerable(leftSource);
        final Enumerable<Row> inner = Linq4j.asEnumerable(rightSource);
        final Function1<Row, Object[]> outerKeySelector = a0 -> {
            Object[] values = new Object[leftKeys.length];
            for (int i = 0; i < values.length; i++) {
                values[i] = a0.values[leftKeys[i]];
            }
            return values;
        };
        final Function1<Row, Object[]> innerKeySelector = a0 -> {
            Object[] values = new Object[rightKeys.length];
            for (int i = 0; i < values.length; i++) {
                values[i] = a0.values[rightKeys[i]];
            }
            return values;
        };
        final Function2<Row, Row, Row> resultSelector = (v0, v1) -> v0.compose(v1);
        final EqualityComparer<Object[]> comparer = new EqualityComparer<Object[]>() {
            @Override
            public boolean equal(Object[] v1, Object[] v2) {
                return Arrays.equals(v1, v2);//todo 隐式转换比较
            }

            @Override
            public int hashCode(Object[] row) {
                return Arrays.hashCode(row);
            }
        };
        Predicate2<Row, Row> predicate = (v0, v1) -> {
            o.values = resultSelector.apply(v0, v1).values;
            return scalar.execute(o) == Boolean.TRUE;
        };

        Enumerable<Row> rows = EnumerableDefaults.hashJoin(
                outer,
                inner,
                outerKeySelector,
                innerKeySelector,
                resultSelector,
                comparer,
                generateNullsOnLeft,
                generateNullsOnRight,
                predicate);
        return new MycatJoinExecutor(executors, rows);
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
        return new MycatAggregateExecutor(input, mycatAggregate);
    }

    @Override
    public Executor implement(MycatUnion mycatUnion) {
        Executor[] executors = implementInputs(mycatUnion);
        if (mycatUnion.all) return new MycatUnionAllExecutor(executors);
        else return new MycatUnionDistinctExecutor(executors);
    }

    @Override
    public Executor implement(MycatIntersect mycatIntersect) {
        return null;
    }

    @Override
    public Executor implement(MycatMinus mycatMinus) {
        return null;
    }

    @Override
    public Executor implement(MycatTableModify mycatTableModify) {
        return null;
    }

    @Override
    public Executor implement(MycatValues mycatValues) {
        return null;
    }

    @Override
    public Executor implement(MycatSort mycatSort) {
        Executor executor = implementInput((MycatRel) mycatSort);
        RelDataType inputRowType = mycatSort.getInput().getRowType();
        RelCollation collation = mycatSort.getCollation();
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        if (!fieldCollations.isEmpty()) {
            Comparator<Row> comparator = comparator(mycatSort);
            executor = new MycatSortExecutor(comparator, executor);
        }
        RexNode offset = mycatSort.offset;
        RexNode fetch = mycatSort.fetch;
        if (offset != null || fetch != null) {
            offset = resolveDynamicParam(offset);
            fetch = resolveDynamicParam(fetch);
            final long offsetValue =
                    offset == null
                            ? 0
                            : ((RexLiteral) offset).getValueAs(Integer.class);
            final long fetchValue =
                    fetch == null
                            ? Long.MAX_VALUE
                            : ((RexLiteral) fetch).getValueAs(Integer.class);
            executor = new MycatLimitExecutor(offsetValue, fetchValue, executor);
        }
        return executor;
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
    public Executor implement(MergeSort mergeSort) {
        return null;
    }

    @Override
    public Executor implement(NestedLoopJoin nestedLoopJoin) {
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
    public Executor implement(SortMergeJoin sortMergeJoin) {
        return null;
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
            }else {
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
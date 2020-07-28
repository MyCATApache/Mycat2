package io.mycat.hbt4.executor;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.MycatContext;
import io.mycat.hbt4.MycatRexCompiler;
import io.mycat.hbt4.physical.MycatSortMergeJoin;
import io.mycat.mpp.Row;
import lombok.SneakyThrows;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.objenesis.instantiator.util.UnsafeUtils;

import java.util.Iterator;

public class MycatMergeJoinExecutor implements Executor {
    private MycatSortMergeJoin sortMergeJoin;
    private final JoinRelType joinType;
    private final Executor outer;
    private final Executor inner;
    private final ImmutableList<RexNode> nonEquiConditions;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final int leftFieldCount;
    private final int rightFieldCount;
    private final RelDataType resultRelDataType;
    private Enumerable<Row> rows;
    private Iterator<Row> iterator;

    public MycatMergeJoinExecutor(MycatSortMergeJoin sortMergeJoin, JoinRelType joinType,
                                  Executor outer,
                                  Executor inner,
                                  ImmutableList<RexNode> nonEquiConditions,
                                  int[] leftKeys,
                                  int[] rightKeys,
                                  int leftFieldCount,
                                  int rightFieldCount,
                                  RelDataType resultRelDataType) {
        this.sortMergeJoin = sortMergeJoin;
        this.joinType = joinType;
        this.outer = outer;
        this.inner = inner;
        this.nonEquiConditions = nonEquiConditions;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.leftFieldCount = leftFieldCount;
        this.rightFieldCount = rightFieldCount;
        this.resultRelDataType = resultRelDataType;
    }

    @Override
    @SneakyThrows
    public void open() {
        if (rows == null) {
            outer.open();
            inner.open();
            MycatContext o = (MycatContext) UnsafeUtils.getUnsafe().allocateInstance(MycatContext.class);
            Enumerable<Row> outerEnumerate = Linq4j.asEnumerable(outer);
            Enumerable<Row> innerEnumerate = Linq4j.asEnumerable(inner);
            final Function1<Row, Row> outerKeySelector = a0 -> {
                Object[] values = new Object[leftKeys.length];
                for (int i = 0; i < values.length; i++) {
                    values[i] = a0.values[leftKeys[i]];
                }
                return Row.of(values);
            };
            final Function1<Row, Row> innerKeySelector = a0 -> {
                Object[] values = new Object[rightKeys.length];
                for (int i = 0; i < values.length; i++) {
                    values[i] = a0.values[rightKeys[i]];
                }
                return Row.of(values);
            };
            final Function2<Row, Row, Row> resultSelector = Row.composeJoinRow(leftFieldCount, rightFieldCount);
            RexNode nonEquiCondition = RexUtil.composeConjunction(
                    this.sortMergeJoin.getCluster().getRexBuilder(),
                    nonEquiConditions, true);
            Predicate2<Row, Row> nonEquiConditionPredicate = null;
            if (nonEquiCondition != null) {
                MycatScalar scalar = MycatRexCompiler.compile(ImmutableList.of(nonEquiCondition), resultRelDataType);
                nonEquiConditionPredicate = (v0, v1) -> {
                    o.values = v0.values;
                    return scalar.execute(o) == Boolean.TRUE;
                };
            } else {
                nonEquiConditionPredicate = (l, r) -> true;
            }
            rows = EnumerableDefaults.mergeJoin(outerEnumerate,
                    innerEnumerate,
                    outerKeySelector,
                    innerKeySelector,
                    nonEquiConditionPredicate,
                    resultSelector,
                    JoinType.valueOf(joinType.name())
                    , null);

        }
        this.iterator = rows.iterator();
    }

    @Override
    public Row next() {
        if (this.iterator.hasNext()) {
            return this.iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        outer.close();
        inner.close();
    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }


}
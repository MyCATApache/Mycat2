package io.mycat.hbt4.executor;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.logical.MycatHashJoin;
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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.objenesis.instantiator.util.UnsafeUtils;

import java.util.Iterator;

public class MycatHashJoinExecutor implements Executor {
    private MycatHashJoin mycatHashJoin;
    private final JoinRelType joinType;
    private Executor outer;
    private Executor inner;
    private final Executor originOuter;
    private final Executor originInner;
    private final ImmutableList<RexNode> nonEquiConditions;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final boolean generateNullsOnLeft;
    private final boolean generateNullsOnRight;
    private final int leftFieldCount;
    private final int rightFieldCount;
    private final RelDataType resultRelDataType;
    private TempResultSetFactory tempResultSetFactory;
    private Enumerable<Row> rows;
    private Iterator<Row> iterator;

    public MycatHashJoinExecutor(MycatHashJoin mycatHashJoin, JoinRelType joinType,
                                 Executor outer,
                                 Executor inner,
                                 ImmutableList<RexNode> nonEquiConditions,
                                 int[] leftKeys,
                                 int[] rightKeys,
                                 boolean generateNullsOnLeft,
                                 boolean generateNullsOnRight,
                                 int leftFieldCount,
                                 int rightFieldCount,
                                 RelDataType resultRelDataType,
                                 TempResultSetFactory tempResultSetFactory) throws InstantiationException {
        this.mycatHashJoin = mycatHashJoin;
        this.joinType = joinType;
        this.originOuter = this.outer = outer;
        this.originInner = this.inner = inner;
        this.nonEquiConditions = nonEquiConditions;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.generateNullsOnLeft = generateNullsOnLeft;
        this.generateNullsOnRight = generateNullsOnRight;
        this.leftFieldCount = leftFieldCount;
        this.rightFieldCount = rightFieldCount;
        this.resultRelDataType = resultRelDataType;
        this.tempResultSetFactory = tempResultSetFactory;
    }

    @Override
    @SneakyThrows
    public void open() {
        if (rows == null) {
            originOuter.open();
            originInner.open();
            Context o = (Context) UnsafeUtils.getUnsafe().allocateInstance(Context.class);
            if (!outer.isRewindSupported()) {
                outer = tempResultSetFactory.makeRewind(outer);
                outer.open();
            }
            if (!inner.isRewindSupported()) {
                inner = tempResultSetFactory.makeRewind(inner);
                inner.open();
            }
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
            final EqualityComparer<Row> compare = null;

           RexNode nonEquiCondition = RexUtil.composeConjunction(
                    this.mycatHashJoin.getCluster().getRexBuilder(),
                    nonEquiConditions, true);
            switch (joinType) {
                case ANTI:
                case SEMI: {
                    Predicate2<Row,Row> predicate2;
                    if (nonEquiCondition != null) {
                        JaninoRexCompiler compiler = new JaninoRexCompiler(MycatCalciteSupport.INSTANCE.RexBuilder);
                        Scalar scalar = compiler.compile(ImmutableList.of(nonEquiCondition), resultRelDataType);
                        predicate2= (v0, v1) -> {
                            o.values = resultSelector.apply((Row) v0, (Row) v1).values;
                            return scalar.execute(o) == Boolean.TRUE;
                        };
                    }else {
                        predicate2 = (i,y)->true;
                    }
                    rows = EnumerableDefaults.semiJoin(outerEnumerate,
                            innerEnumerate,
                            outerKeySelector,
                            innerKeySelector,
                            compare, joinType == JoinRelType.ANTI,
                            predicate2);
                    break;
                }
                default:
                    rows = EnumerableDefaults.hashJoin(outerEnumerate, innerEnumerate, outerKeySelector, innerKeySelector,
                            resultSelector, compare, generateNullsOnLeft, generateNullsOnRight);
                    if (nonEquiCondition != null) {
                        JaninoRexCompiler compiler = new JaninoRexCompiler(MycatCalciteSupport.INSTANCE.RexBuilder);
                        Scalar scalar = compiler.compile(ImmutableList.of(nonEquiCondition), resultRelDataType);
                        rows = rows.where(v0 -> {
                            o.values = v0.values;
                            return scalar.execute(o) == Boolean.TRUE;
                        });
                    }
                    break;
            }

        }
        this.iterator = rows.iterator();
    }

    @Override
    public Row next() {
        if (this.iterator.hasNext()){
            return this.iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        originInner.close();
        originOuter.close();
        outer.close();
        inner.close();
    }

    @Override
    public boolean isRewindSupported() {
        return true;
    }



}
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
package io.mycat.hbt4.executor;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.hbt4.MycatRexCompiler;
import io.mycat.mpp.Row;
import lombok.SneakyThrows;
import org.apache.calcite.MycatContext;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class MycatHashJoinExecutor implements Executor {
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
    private List<Object> params;
    private Enumerable<Row> rows;
    private Iterator<Row> iterator;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatHashJoinExecutor.class);

    public MycatHashJoinExecutor(JoinRelType joinType,
                                 Executor outer,
                                 Executor inner,
                                 ImmutableList<RexNode> nonEquiConditions,
                                 int[] leftKeys,
                                 int[] rightKeys,
                                 boolean generateNullsOnLeft,
                                 boolean generateNullsOnRight,
                                 int leftFieldCount,
                                 int rightFieldCount,
                                 RelDataType resultRelDataType, List<Object> params) {
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
        this.params = params;

        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("create MycatMergeJoinExecutor ");
        }
    }

    public MycatHashJoinExecutor create(
            JoinRelType joinType,
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
            List<Object> params
    ) {
        return new MycatHashJoinExecutor(
                joinType,
                outer,
                inner,
                nonEquiConditions,
                leftKeys,
                rightKeys,
                generateNullsOnLeft,
                generateNullsOnRight,
                leftFieldCount,
                rightFieldCount,
                resultRelDataType,
                params
        );
    }

    @Override
    @SneakyThrows
    public void open() {
        if (rows == null) {
            originOuter.open();
            originInner.open();
            MycatContext o = new MycatContext();
////////////////////////////////////check////////////////////////////////////////////////
//            if (!outer.isRewindSupported()) {
//                outer = tempResultSetFactory.makeRewind(outer);
//                outer.open();
//            }
//            if (!inner.isRewindSupported()) {
//                inner = tempResultSetFactory.makeRewind(inner);
//                inner.open();
//            }
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
                    MycatCalciteSupport.INSTANCE.RexBuilder,
                    nonEquiConditions, true);
            switch (joinType) {
                case ANTI:
                case SEMI: {
                    Predicate2<Row, Row> predicate2;
                    if (nonEquiCondition != null) {
                        MycatScalar scalar = MycatRexCompiler.compile(ImmutableList.of(nonEquiCondition),
                                resultRelDataType,params);
                        predicate2 = (v0, v1) -> {
                            o.values = resultSelector.apply(v0, v1).values;
                            return scalar.execute(o) == Boolean.TRUE;
                        };
                    } else {
                        predicate2 = (i, y) -> true;
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
                        MycatScalar scalar = MycatRexCompiler
                                .compile(ImmutableList.of(nonEquiCondition), resultRelDataType,params);
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
        if (this.iterator.hasNext()) {
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


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        writer.item("joinType",joinType);
        originOuter.explain(writer);
        originInner.explain(writer);
        return explainWriter.ret();
    }
}
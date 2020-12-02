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

import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.mpp.Row;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate2;

import java.util.Iterator;
import java.util.List;

public class MycatBatchNestedLoopJoinExecutor implements Executor {
    final JoinType joinType;
    final Executor leftInput;
    MycatLookupExecutor rightInput;
    final Executor originRightInput;
    final int leftExecuterFieldCount;
    final int rightExecuterFieldCount;
    final static int batchSize = 1000;
    final Predicate2<Row, Row> lookup;
    final Predicate2<Row, Row> nonEqualCondition;
    private Iterator<Row> iterator;
    private Enumerable<Row> leftEnumerable;
    Function1<List<Row>, Enumerable<Row>> inner;

    protected MycatBatchNestedLoopJoinExecutor(JoinType joinType,
                                            Executor leftInput,
                                            MycatLookupExecutor rightInput,
                                            int leftExecuterFieldCount,
                                            int rightExecuterFieldCount,
                                            Predicate2<Row, Row> lookup,
                                            Predicate2<Row, Row> nonEqualCondition) {
        this.joinType = joinType;
        this.leftInput = leftInput;
        this.originRightInput = rightInput;
        this.rightInput = rightInput;
        this.leftExecuterFieldCount = leftExecuterFieldCount;
        this.rightExecuterFieldCount = rightExecuterFieldCount;
        this.lookup = lookup;
        this.nonEqualCondition = nonEqualCondition;
    }

    public static MycatBatchNestedLoopJoinExecutor create(
            JoinType joinType,
            Executor leftInput,
            MycatLookupExecutor rightInput,
            int leftExecuterFieldCount,
            int rightExecuterFieldCount,
            Predicate2<Row, Row> lookup,
            Predicate2<Row, Row> nonEqualCondition) {
        return new MycatBatchNestedLoopJoinExecutor(
                joinType,
                leftInput,
                rightInput,
                leftExecuterFieldCount,
                rightExecuterFieldCount,
                lookup,
                nonEqualCondition
        );
    }

    @Override
    public void open() {
        if (this.iterator == null) {
            leftInput.open();
            this.leftEnumerable = Linq4j.asEnumerable(leftInput);
            inner = inlist -> {
                rightInput.setIn(inlist);
                rightInput.open();
                return  Linq4j.asEnumerable(rightInput);
            };
        }
        this.iterator = EnumerableDefaults.correlateBatchJoin(joinType,
                leftEnumerable,
                inner,
                (v0, v1) -> {
                    if (v0 == null) {
                        v0 = Row.create(leftExecuterFieldCount);
                    }
                    if (v1 == null) {
                        v1 = Row.create(rightExecuterFieldCount);
                    }
                    return v0.compose(v1);
                },
                nonEqualCondition, batchSize).iterator();
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
        if (this.rightInput != null) {
            this.rightInput.close();
            this.rightInput = null;
        }
        if (this.leftInput != null) {
            this.leftInput.close();
        }
        if (this.originRightInput != null) {
            this.originRightInput.close();
        }
    }

    @Override
    public boolean isRewindSupported() {
        return this.leftInput.isRewindSupported();
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        explainWriter.item("joinType",joinType);
        leftInput.explain(explainWriter);
        rightInput.explain(explainWriter);
        return explainWriter.ret();
    }
}
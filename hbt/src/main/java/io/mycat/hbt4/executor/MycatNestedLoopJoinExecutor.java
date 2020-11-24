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
import io.mycat.mpp.Row;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.Iterator;

public class MycatNestedLoopJoinExecutor implements Executor {
    private final JoinRelType joinType;
    private final Executor leftSource;
    private Executor rightSource;
    private final Executor originalRightSource;
    private final Function2<Row, Row, Row> resultSelector;
    private final Predicate2<Row, Row> predicate;
    private TempResultSetFactory tempResultSetFactory;
    private Iterator<Row> iterator;

    protected MycatNestedLoopJoinExecutor(
            JoinRelType joinType,
            Executor leftSource,
            Executor rightSource,
            Function2<Row, Row, Row> resultSelector,
            Predicate2<Row, Row> predicate,
            TempResultSetFactory tempResultSetFactory) {
        this.joinType = joinType;
        this.leftSource = leftSource;
        this.rightSource = originalRightSource = rightSource;
        this.resultSelector = resultSelector;
        this.predicate = predicate;
        this.tempResultSetFactory = tempResultSetFactory;
    }

    public static MycatNestedLoopJoinExecutor create(
            JoinRelType joinType,
            Executor leftSource,
            Executor rightSource,
            Function2<Row, Row, Row> resultSelector,
            Predicate2<Row, Row> predicate,
            TempResultSetFactory tempResultSetFactory
    ) {
        return new MycatNestedLoopJoinExecutor(
                joinType,
                leftSource,
                rightSource,
                resultSelector,
                predicate,
                tempResultSetFactory
        );
    }

    @Override
    public void open() {
        JoinType joinType = JoinType.valueOf(this.joinType.name());
        if (iterator == null) {
            leftSource.open();
            rightSource.open();
            if (!joinType.generatesNullsOnLeft() && !rightSource.isRewindSupported()) {
                rightSource = tempResultSetFactory.makeRewind(originalRightSource);
                rightSource.open();
                originalRightSource.close();
            }
        }
        this.iterator = EnumerableDefaults.nestedLoopJoinAsList(
                Linq4j.asEnumerable(leftSource),
                Linq4j.asEnumerable(rightSource),
                predicate, resultSelector, joinType)
                .iterator();
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        leftSource.close();
        originalRightSource.close();
        if (rightSource != null) {
            rightSource.close();
        }
        rightSource = null;
    }

    @Override
    public boolean isRewindSupported() {
        return leftSource.isRewindSupported();
    }
}
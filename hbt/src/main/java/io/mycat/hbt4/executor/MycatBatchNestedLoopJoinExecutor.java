package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.hbt4.MycatContext;
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
    private MycatContext context;
    final JoinType joinType;
    final Executor leftInput;
    MycatLookupExecutor rightInput;
    final Executor originRightInput;
    final int leftExecuterFieldCount;
    final int rightExecuterFieldCount;
    final static int batchSize = 1000;
    final Predicate2<Row, Row> lookup;
    final Predicate2<Row, Row> nonEqualCondition;
    final TempResultSetFactory tempResultSetFactory;
    private Iterator<Row> iterator;
    private Enumerable<Row> leftEnumerable;
    Function1<List<Row>, Enumerable<Row>> inner;

    public MycatBatchNestedLoopJoinExecutor(JoinType joinType,
                                            Executor leftInput,
                                            MycatLookupExecutor rightInput,
                                            int leftExecuterFieldCount,
                                            int rightExecuterFieldCount,
                                            Predicate2<Row, Row> lookup,
                                            Predicate2<Row, Row> nonEqualCondition,
                                            TempResultSetFactory tempResultSetFactory,
                                            MycatContext context) {
        this.joinType = joinType;
        this.leftInput = leftInput;
        this.originRightInput = rightInput;
        this.rightInput = rightInput;
        this.leftExecuterFieldCount = leftExecuterFieldCount;
        this.rightExecuterFieldCount = rightExecuterFieldCount;
        this.lookup = lookup;
        this.nonEqualCondition = nonEqualCondition;
        this.tempResultSetFactory = tempResultSetFactory;
        this.context = context;
    }

    public static MycatBatchNestedLoopJoinExecutor create(
            JoinType joinType,
            Executor leftInput,
            MycatLookupExecutor rightInput,
            int leftExecuterFieldCount,
            int rightExecuterFieldCount,
            Predicate2<Row, Row> lookup,
            Predicate2<Row, Row> nonEqualCondition,
            TempResultSetFactory tempResultSetFactory,
            MycatContext context) {
        return new MycatBatchNestedLoopJoinExecutor(
                joinType,
                leftInput,
                rightInput,
                leftExecuterFieldCount,
                rightExecuterFieldCount,
                lookup,
                nonEqualCondition,
                tempResultSetFactory,
                context);
    }

    @Override
    public void open() {
        if (this.iterator == null) {
            leftInput.open();
//            if (!rightInput.isRewindSupported()) {
//                rightInput.open();
//                rightInput = tempResultSetFactory.makeRewind(rightInput);
//                originRightInput.close();
//            }
            rightInput.open();
            this.leftEnumerable = Linq4j.asEnumerable(leftInput);
            Enumerable<Row> rightEnumerable = Linq4j.asEnumerable(rightInput);
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
}
package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.function.Predicate2;

public class MycatBatchNestedLoopJoinExecutor extends AbstractBatchNestedLoopJoin {
    public MycatBatchNestedLoopJoinExecutor(JoinType joinType,
                                            Executor leftInput,
                                            Executor rightInput,
                                            int leftExecuterFieldCount,
                                            int rightExecuterFieldCount,
                                            Predicate2<Row, Row> lookup,
                                            Predicate2<Row, Row> nonEqualCondition,
                                            TempResultSetFactory tempResultSetFactory) {
        super(joinType, leftInput, rightInput, leftExecuterFieldCount, rightExecuterFieldCount, lookup, nonEqualCondition, tempResultSetFactory);
    }
}
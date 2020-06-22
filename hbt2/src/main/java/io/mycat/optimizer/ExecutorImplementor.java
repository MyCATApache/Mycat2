package io.mycat.optimizer;

import io.mycat.optimizer.logical.*;
import io.mycat.optimizer.physical.*;

public interface ExecutorImplementor {
    Executor implement(BottomTable bottomTable);

    Executor implement(BottomView bottomView);

    Executor implement(MycatJoin mycatJoin);

    Executor implement(MycatCalc mycatCalc);

    Executor implement(MycatProject mycatProject);

    Executor implement(MycatFilter mycatFilter);

    Executor implement(MycatAggregate mycatAggregate);

    Executor implement(MycatUnion mycatUnion);

    Executor implement(MycatIntersect mycatIntersect);

    Executor implement(MycatMinus mycatMinus);

    Executor implement(MycatTableModify mycatTableModify);

    Executor implement(MycatValues mycatValues);

    Executor implement(MycatSort mycatSort);

    Executor implement(GatherView gatherView);

    Executor implement(BKAJoin bkaJoin);

    Executor implement(HashAgg hashAgg);

    Executor implement(HashJoin hashJoin);

    Executor implement(MaterializedSemiJoin materializedSemiJoin);

    Executor implement(MemSort memSort);

    Executor implement(MergeSort mergeSort);

    Executor implement(NestedLoopJoin nestedLoopJoin);

    Executor implement(SemiHashJoin semiHashJoin);

    Executor implement(SortAgg sortAgg);

    Executor implement(SortMergeJoin sortMergeJoin);

    Executor implement(SortMergeSemiJoin sortMergeSemiJoin);

    Executor implement(TopN topN);
}
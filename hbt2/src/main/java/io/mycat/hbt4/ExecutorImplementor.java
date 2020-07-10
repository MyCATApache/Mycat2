package io.mycat.hbt4;


import io.mycat.hbt3.MultiView;
import io.mycat.hbt3.View;
import io.mycat.hbt4.logical.*;
import io.mycat.hbt4.physical.*;

public interface ExecutorImplementor {

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

    Executor implement(QueryView gatherView);

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

    Executor implement(MycatQuery mycatQuery);

    Executor implement(MultiView multiView);

    Executor implement(View view);

//    Executor implement(BottomView bottomView);
}
package io.mycat.optimizer;

import io.mycat.optimizer.logical.*;
import io.mycat.optimizer.physical.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;

public interface ExecutorImplementor {
    Executor implement(Aggregate aggregate);

    Executor implement(Filter filter);

    Executor implement(Correlate correlate);

    Executor implement(Join join);

    Executor implement(Project project);

    Executor implement(Sort sort);

    Executor implement(TableScan tableScan);

    Executor implement(Union union);

    Executor implement(Values values);

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
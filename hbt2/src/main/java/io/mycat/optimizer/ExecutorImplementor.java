package io.mycat.optimizer;

public interface ExecutorImplementor {
    Executor implement(BottomTable bottomTable);

    Executor implement(BottomView bottomView);

    Executor implement(MycatRules.MycatJoin mycatJoin);

    Executor implement(MycatRules.MycatCalc mycatCalc);

    Executor implement(MycatRules.MycatProject mycatProject);

    Executor implement(MycatRules.MycatFilter mycatFilter);

    Executor implement(MycatRules.MycatAggregate mycatAggregate);

    Executor implement(MycatRules.MycatUnion mycatUnion);

    Executor implement(MycatRules.MycatIntersect mycatIntersect);

    Executor implement(MycatRules.MycatMinus mycatMinus);

    Executor implement(MycatRules.MycatTableModify mycatTableModify);

    Executor implement(MycatRules.MycatValues mycatValues);

    Executor implement(MycatRules.MycatSort mycatSort);

    Executor implement(GatherView gatherView);
}
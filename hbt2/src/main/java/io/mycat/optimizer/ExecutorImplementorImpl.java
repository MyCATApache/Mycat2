package io.mycat.optimizer;

import org.apache.calcite.rel.RelNode;

public class ExecutorImplementorImpl implements ExecutorImplementor {
    @Override
    public Executor implement(BottomTable bottomTable) {
        return null;
    }

    @Override
    public Executor implement(BottomView bottomView) {
        RelNode relNode = bottomView.getRelNode();
        String sql = bottomView.getSql();

        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatJoin mycatJoin) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatCalc mycatCalc) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatProject mycatProject) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatFilter mycatFilter) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatAggregate mycatAggregate) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatUnion mycatUnion) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatIntersect mycatIntersect) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatMinus mycatMinus) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatTableModify mycatTableModify) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatValues mycatValues) {
        return null;
    }

    @Override
    public Executor implement(MycatRules.MycatSort mycatSort) {
        return null;
    }

    @Override
    public Executor implement(GatherView gatherView) {
        return null;
    }
}
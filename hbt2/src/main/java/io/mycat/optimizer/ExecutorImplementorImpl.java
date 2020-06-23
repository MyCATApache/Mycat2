package io.mycat.optimizer;

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.mpp.plan.LimitPlan;
import io.mycat.optimizer.logical.*;
import io.mycat.optimizer.physical.*;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;

public class ExecutorImplementorImpl implements ExecutorImplementor {
    @Override
    public Executor implement(BottomTable bottomTable) {
        return new Executor() {
        };
    }

    @Override
    public Executor implement(BottomView bottomView) {
        RelNode relNode = bottomView.getRelNode();
        String sql = bottomView.getSql();

        return null;
    }

    @Override
    public Executor implement(MycatJoin mycatJoin) {
        return null;
    }

    @Override
    public Executor implement(MycatCalc mycatCalc) {
//        Expression expression = RexToLixTranslator.translateCondition();
//        RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
//        JaninoRexCompiler janinoRexCompiler = new JaninoRexCompiler(rexBuilder);
//        janinoRexCompiler.compile()
        final BlockBuilder builder2 = new BlockBuilder();
        return null;
    }

    @Override
    public Executor implement(MycatProject mycatProject) {
        return null;
    }

    @Override
    public Executor implement(MycatFilter mycatFilter) {
        return null;
    }

    @Override
    public Executor implement(MycatAggregate mycatAggregate) {
        return null;
    }

    @Override
    public Executor implement(MycatUnion mycatUnion) {
        return null;
    }

    @Override
    public Executor implement(MycatIntersect mycatIntersect) {
        return null;
    }

    @Override
    public Executor implement(MycatMinus mycatMinus) {
        return null;
    }

    @Override
    public Executor implement(MycatTableModify mycatTableModify) {
        return null;
    }

    @Override
    public Executor implement(MycatValues mycatValues) {
        return null;
    }

    @Override
    public Executor implement(MycatSort mycatSort) {
        return null;
    }

    @Override
    public Executor implement(GatherView gatherView) {
        return null;
    }

    @Override
    public Executor implement(BKAJoin bkaJoin) {
        return null;
    }

    @Override
    public Executor implement(HashAgg hashAgg) {
        return null;
    }

    @Override
    public Executor implement(HashJoin hashJoin) {
        return null;
    }

    @Override
    public Executor implement(MaterializedSemiJoin materializedSemiJoin) {
        return null;
    }

    @Override
    public Executor implement(MemSort memSort) {
        return null;
    }

    @Override
    public Executor implement(MergeSort mergeSort) {
        return null;
    }

    @Override
    public Executor implement(NestedLoopJoin nestedLoopJoin) {
        return null;
    }

    @Override
    public Executor implement(SemiHashJoin semiHashJoin) {
        return null;
    }

    @Override
    public Executor implement(SortAgg sortAgg) {
        return null;
    }

    @Override
    public Executor implement(SortMergeJoin sortMergeJoin) {
        return null;
    }

    @Override
    public Executor implement(SortMergeSemiJoin sortMergeSemiJoin) {
        return null;
    }

    @Override
    public Executor implement(TopN topN) {
        return null;
    }
}
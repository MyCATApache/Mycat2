package io.mycat.hbt4.logical;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

public class MycatHashJoin extends Join implements MycatRel {
    protected MycatHashJoin(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    }

    public static MycatHashJoin create(RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        RelOptCluster cluster = left.getCluster();
        RelTraitSet relTraitSet = cluster.traitSetOf(MycatConvention.INSTANCE);
        return new MycatHashJoin(cluster,
                relTraitSet, ImmutableList.of(),left,right,condition,variablesSet,joinType);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatHashJoin").into().ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return create(left,right,conditionExpr,variablesSet,joinType);
    }
}
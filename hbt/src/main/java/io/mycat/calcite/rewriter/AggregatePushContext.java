package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import lombok.Getter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;

/**
 * reference from
 * com.alibaba.polardbx.optimizer.core.planner.rule.CBOPushAggRule#tryPushAgg
 * com.alibaba.polardbx.optimizer.core.planner.rule.CBOPushAggRule#splitAgg
 *
 * Refactoring to fit mycat
 */
@Getter
public class AggregatePushContext {
    final Aggregate aggregate;
    final List<RexNode> projectExprList;
    final List<AggregateCall> globalAggregateCallList;
    final List<AggregateCall> partialAggregateCallList;

    public AggregatePushContext(Aggregate aggregate) {
        this.aggregate = aggregate;
        this.projectExprList = createProjectFromAggregate(aggregate);
        this.globalAggregateCallList = new ArrayList<>();
        this.partialAggregateCallList = new ArrayList<>();
    }

    public static AggregatePushContext split(Aggregate aggregate){
        AggregatePushContext aggregatePushContext = new AggregatePushContext(aggregate);
        aggregatePushContext.split();
        return aggregatePushContext;
    }

    public void split() {
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            SqlAggFunction function = aggregateCall.getAggregation();
            switch (function.getKind()) {
                case COUNT:
                    splitCount(aggregateCall);
                    break;
                case AVG:
                    splitAvg(aggregateCall);
                    break;
                case MIN:
                case MAX:
                case SUM:
                case BIT_OR:
                case BIT_XOR:
                case BIT_AND:
                    splitCommon(aggregateCall);
                    break;
                default:
                    throw new UnsupportedOperationException(function.toString());
            }
        }
    }

    private static List<RexNode> createProjectFromAggregate(Aggregate aggregate) {
        List<RexNode> projectList = new ArrayList<>();
        for (int i = 0; i < aggregate.getGroupSet().cardinality(); i++) {
            projectList.add(new RexInputRef(i, aggregate.getRowType().getFieldList().get(i).getType()));
        }
        return projectList;
    }

    private  void splitCommon(AggregateCall aggCall) {
        AggregateCall newAggCall =
                aggCall.copy(ImmutableIntList.of(aggregate.getGroupSet().cardinality() + partialAggregateCallList.size()), -1, aggCall.getCollation());
        globalAggregateCallList.add(newAggCall);

        projectExprList.add(new RexInputRef(aggregate.getGroupSet().cardinality() + partialAggregateCallList.size(), aggCall.getType()));

        partialAggregateCallList.add(aggCall);
    }

    private  void splitAvg(AggregateCall aggregateCall) {
        AggregateCall partialSumAggCall = AggregateCall.create(
                SqlStdOperatorTable.SUM,
                aggregateCall.isDistinct(),
                aggregateCall.isApproximate(),
                false,
                aggregateCall.getArgList(),
                aggregateCall.filterArg,
                aggregateCall.getCollation(),
                aggregateCall.getType(),
                "PartialSum");

        AggregateCall globalSumAggCall =
                partialSumAggCall.copy(ImmutableIntList.of(aggregate.getGroupSet().cardinality() + partialAggregateCallList.size()),
                        -1,
                        aggregate.getTraitSet().getCollation());

        partialAggregateCallList.add(partialSumAggCall);

        AggregateCall partialCountAggCall = AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                aggregateCall.isDistinct(),
                aggregateCall.isApproximate(),
                false,
                aggregateCall.getArgList(),
                aggregateCall.filterArg,
                aggregateCall.getCollation(),
                MycatCalciteSupport.TypeFactory.createSqlType(SqlTypeName.BIGINT),
                "PartialCount");

        AggregateCall globalCountAggCall = AggregateCall.create(
                SqlStdOperatorTable.SUM,
                partialCountAggCall.isDistinct(),
                partialCountAggCall.isApproximate(),
                false,
                ImmutableIntList.of(aggregate.getGroupSet().cardinality() + partialAggregateCallList.size()),
                partialCountAggCall.filterArg,
                aggregateCall.getCollation(),
                partialCountAggCall.getType(),
                "GlobalSumCount");

        partialAggregateCallList.add(partialCountAggCall);

        RexInputRef partialSumRef =
                new RexInputRef(globalSumAggCall.getArgList().get(0), partialSumAggCall.getType());
        RexInputRef partialCountRef =
                new RexInputRef(globalCountAggCall.getArgList().get(0), partialCountAggCall.getType());

        RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        RexCall divide = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
                partialSumRef,
                partialCountRef);

        RelDataType relDataType = aggregateCall.getType();
        if (!divide.getType().getSqlTypeName().equals(relDataType.getSqlTypeName())) {
            RexNode castRexNode = rexBuilder.makeCast(relDataType, divide);
            projectExprList.add(castRexNode);
        } else {
            projectExprList.add(divide);
        }

        globalAggregateCallList.add(globalSumAggCall);
        globalAggregateCallList.add(globalCountAggCall);
    }

    private  void splitCount(AggregateCall aggCall) {
        AggregateCall sumAggregateCall = AggregateCall.create(
                SqlStdOperatorTable.SUM0,
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                false,
                ImmutableList.of(aggregate.getGroupSet().cardinality() + partialAggregateCallList.size()),
                aggCall.filterArg,
                aggCall.getCollation(),
                aggCall.getType(),
                aggCall.getName());

        globalAggregateCallList.add(sumAggregateCall);
        projectExprList.add(new RexInputRef(aggregate.getGroupSet().cardinality() + partialAggregateCallList.size(), aggCall.getType()));
        partialAggregateCallList.add(aggCall);
    }
}
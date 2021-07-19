package io.mycat.calcite.rules;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatRelBuilder;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatSQLTableLookup;
import io.mycat.calcite.rewriter.Distribution;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBeans;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.mycat.calcite.MycatImplementor.MYCAT_SQL_LOOKUP_IN;

public class MycatTableLookupSemiJoinRule extends RelRule<MycatTableLookupSemiJoinRule.Config> {

    public static final MycatTableLookupSemiJoinRule INSTANCE = Config.DEFAULT.toRule();

    public MycatTableLookupSemiJoinRule(Config config) {
        super(config);
    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelOptCluster cluster = join.getCluster();
        RelNode left = call.rel(1);
        RelNode right = call.rel(2);

        if (!join.analyzeCondition().isEqui()) {
            return;
        }
        if (!(right instanceof MycatView)) {
            return;
        }
        MycatView mycatView = (MycatView) right;
        if (mycatView.isMergeSort() || mycatView.isMergeAgg()) {
            return;
        }
        JoinRelType joinType = join.getJoinType();
        switch (joinType) {
            case LEFT:
            case INNER:
            case SEMI:
                break;
            default:
                return;
        }
        if (RelOptUtil.countJoins(mycatView.getRelNode())>1){
            return;
        }
        RelBuilder relBuilder = MycatCalciteSupport.relBuilderFactory.create(cluster, null);
//
//        ImmutableList.Builder<RelDataTypeField> listBuilder = ImmutableList.builder();
//        Map<Integer, Integer> sourcePosToTargetPos = new HashMap<>();
//        extractedTrimJoinLeftKeys(join, listBuilder, sourcePosToTargetPos);
//        RelRecordType argTypeListRecordType = new RelRecordType(listBuilder.build());
//        Mapping mapping = Mappings.bijection(sourcePosToTargetPos);
//        RexNode equiCondition = RexUtil.apply(mapping, RelOptUtil.createEquiJoinCondition(right, join.analyzeCondition().rightKeys, left, join.analyzeCondition().leftKeys,
//                MycatCalciteSupport.RexBuilder));
        RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        relBuilder.push(right);
        List<RexNode> rightExprs = new ArrayList<>();
        {
            for (Integer rightKey : join.analyzeCondition().rightSet()) {
                rightExprs.add(relBuilder.field(rightKey));
            }
        }
        List<RexNode> leftExprs = new ArrayList<>();
        List<CorrelationId> correlationIds = new ArrayList<>();
        {
            for (Integer leftKey : join.analyzeCondition().leftSet()) {
                CorrelationId correl = cluster.createCorrel();
                correlationIds.add(correl);
                RelDataType type = left.getRowType().getFieldList().get(leftKey).getType();
                RexNode rexNode = rexBuilder.makeCorrel(typeFactory.createUnknownType(), correl);
                leftExprs.add(rexBuilder.makeCast(type, rexNode));
            }
        }
        RexNode condition = relBuilder.call(MYCAT_SQL_LOOKUP_IN,
                rexBuilder.makeCall(SqlStdOperatorTable.ROW, rightExprs),
                rexBuilder.makeCall(SqlStdOperatorTable.ROW, leftExprs));
        Distribution.Type type = mycatView.getDistribution().type();
        switch (type) {
            case PHY:
            case BROADCAST: {
                RelNode relNode = mycatView.getRelNode();
                relBuilder.push(relNode);
                relBuilder.filter(condition);
                relBuilder.rename(mycatView.getRowType().getFieldNames());
                MycatView view = mycatView.changeTo(relBuilder.build());
                call.transformTo(new MycatSQLTableLookup(cluster, join.getTraitSet(), left, view, joinType, join.getCondition(), correlationIds, MycatSQLTableLookup.Type.BACK));
                return;
            }
            case SHARDING: {
                RelNode innerRelNode = mycatView.getRelNode();
                boolean bottomFilter = innerRelNode instanceof TableScan;

                relBuilder.push(mycatView.getRelNode());
                relBuilder.filter(condition);

                RelNode innerDataNode = relBuilder
                        .rename(mycatView.getRowType().getFieldNames())
                        .build();

                Optional<RexNode> viewConditionOptional = mycatView.getCondition();
                RexNode finalCondition = null;
                if (!viewConditionOptional.isPresent() && bottomFilter) {
                    finalCondition = condition;
                } else if (bottomFilter) {
                    finalCondition =
                            viewConditionOptional
                                    .map(i -> RexUtil.composeConjunction(MycatCalciteSupport.RexBuilder, ImmutableList.of(i, condition))).orElse(condition);

                }
                MycatView resView = MycatView.ofCondition(innerDataNode, mycatView.getDistribution(), finalCondition);
                call.transformTo(new MycatSQLTableLookup(cluster, join.getTraitSet(), left, resView, joinType, join.getCondition(), correlationIds, MycatSQLTableLookup.Type.BACK));
                break;
            }
            default:
        }
    }

    public static void extractedTrimJoinLeftKeys(Join join, ImmutableList.Builder<RelDataTypeField> listBuilder, Map<Integer, Integer> oldToNew) {
        int index = 0;
        for (Integer integer : join.analyzeCondition().leftSet()) {
            RelDataTypeField relDataTypeField = join.getInputs().get(0).getRowType().getFieldList().get(integer);
            RelDataTypeFieldImpl relDataTypeField1 = new RelDataTypeFieldImpl(
                    relDataTypeField.getName(),
                    index++,
                    relDataTypeField.getType());
            listBuilder.add(relDataTypeField);
            oldToNew.put(integer, relDataTypeField1.getIndex());
        }
    }

    public interface Config extends RelRule.Config {

        MycatTableLookupSemiJoinRule.Config DEFAULT = EMPTY
                .as(MycatTableLookupSemiJoinRule.Config.class)
                .withOperandFor(b0 ->
                        b0.operand(Join.class).inputs(b1 -> b1.operand(RelNode.class).anyInputs(), b1 -> b1.operand(MycatView.class).noInputs()))
                .withDescription("MycatTableLookupSemiJoinRule")
                .as(MycatTableLookupSemiJoinRule.Config.class);


        @Override
        default MycatTableLookupSemiJoinRule toRule() {
            return new MycatTableLookupSemiJoinRule(this);
        }

        default MycatTableLookupSemiJoinRule.Config withOperandFor(OperandTransform transform) {
            return withOperandSupplier(transform)
                    .as(MycatTableLookupSemiJoinRule.Config.class);
        }
    }
}

package io.mycat.calcite.rules;

import com.google.common.collect.ImmutableList;
import io.mycat.HintTools;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.localrel.LocalFilter;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatSQLTableLookup;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.ShardingTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.IntPair;

import java.util.*;

import static io.mycat.DrdsSqlCompiler.BKA_JOIN_LEFT_ROW_COUNT_LIMIT;
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


        RelMetadataQuery metadataQuery = cluster.getMetadataQuery();
        RelHint lastJoinHint = HintTools.getLastJoinHint(join.getHints());
//        if (lastJoinHint == null){
//            return;
//        }
        boolean hint = false;
        if (lastJoinHint != null && "use_bka_join".equalsIgnoreCase(lastJoinHint.hintName)) {
            hint = true;
        } else {
            double leftRowCount = Optional.ofNullable(metadataQuery.getRowCount(left)).orElse(0.0);
            if (leftRowCount > BKA_JOIN_LEFT_ROW_COUNT_LIMIT) {
                return;
            }
        }
        if (!join.analyzeCondition().isEqui()) {
            return;
        }
        if (!(right instanceof MycatView)) {
            return;
        }
        MycatView mycatView = (MycatView) right;
        if (mycatView.banPushdown()) {
            return;
        }
        if (!hint) {
            if (!isGisView(mycatView)) return;
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
        if (RelOptUtil.countJoins(mycatView.getRelNode()) > 1) {
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
        List<RexNode> rightExprs = new ArrayList<>();
        List<RexNode> leftExprs = new ArrayList<>();
        List<CorrelationId> correlationIds = new ArrayList<>();

        List<IntPair> pairs = join.analyzeCondition().pairs();
        Set<String> orginalTableSet = new HashSet<>();
        for (IntPair pair : pairs) {
            RelColumnOrigin columnOrigin = mycatView.getCluster().getMetadataQuery().getColumnOrigin(mycatView.getRelNode(), pair.target);
            if (columnOrigin != null && !columnOrigin.isDerived()) {
                int originColumnOrdinal = columnOrigin.getOriginColumnOrdinal();
                MycatLogicTable mycatLogicTable = (MycatLogicTable) columnOrigin.getOriginTable().unwrap(MycatLogicTable.class);
                orginalTableSet.add(mycatLogicTable.logicTable().getUniqueName());
                if (orginalTableSet.size() > 1) {
                    return;//右表不能是多个
                }
                String columnName = mycatLogicTable.getRowType().getFieldNames().get(originColumnOrdinal);
                RexInputRef rexInputRef = new RexInputRef(originColumnOrdinal, mycatLogicTable.getRowType().getFieldList().get(originColumnOrdinal).getType());
                rightExprs.add(rexInputRef);

                CorrelationId correl = cluster.createCorrel();
                correlationIds.add(correl);
                RelDataType type = left.getRowType().getFieldList().get(pair.source).getType();
                RexNode rexNode = rexBuilder.makeCorrel(typeFactory.createUnknownType(), correl);
                leftExprs.add(rexBuilder.makeCast(type, rexNode));

            } else {
                continue;//不是原始字段，跳过
            }
        }
        if (rightExprs.isEmpty()) {
            return;
        }

        RexNode condition = relBuilder.call(MYCAT_SQL_LOOKUP_IN,
                rexBuilder.makeCall(SqlStdOperatorTable.ROW, rightExprs),
                rexBuilder.makeCall(SqlStdOperatorTable.ROW, leftExprs));

        RelNode newInnerNode = mycatView.getRelNode();
        if (newInnerNode instanceof TableScan) {
            TableScan tableScan = (TableScan) newInnerNode;
            newInnerNode = LocalFilter.create(condition, tableScan);
        } else {
            newInnerNode = newInnerNode.accept(new RelShuttleImpl() {
                @Override
                public RelNode visit(RelNode other) {
                    if (other instanceof Filter) {
                        Filter filter = (Filter) other;
                        RelNode input = filter.getInput();
                        if (input instanceof TableScan) {
                            TableScan tableScan = (TableScan) input;
                            RexNode condition1 = filter.getCondition();
                            RexNode rexNode = RexUtil.composeConjunction(rexBuilder, ImmutableList.of(condition, condition1));
                            return LocalFilter.create(rexNode, tableScan);
                        }
                    } else if (other.getInputs().size() == 1 && other.getInputs().get(0) instanceof TableScan) {
                        TableScan tableScan = (TableScan) other.getInputs().get(0);
                        return other.copy(other.getTraitSet(), ImmutableList.of(LocalFilter.create(condition, tableScan)));
                    }
                    return super.visit(other);
                }
            });
        }

        relBuilder.push(newInnerNode);
        relBuilder.rename(mycatView.getRowType().getFieldNames());
        newInnerNode = relBuilder.build();
        Distribution.Type type = mycatView.getDistribution().type();
        switch (type) {
            case PHY:
            case BROADCAST: {
                MycatView view = mycatView.changeTo(newInnerNode);
                call.transformTo(new MycatSQLTableLookup(cluster, join.getTraitSet(), left, view, joinType, join.getCondition(), correlationIds, MycatSQLTableLookup.Type.BACK));
                return;
            }
            case SHARDING: {
                MycatView resView = MycatView.ofCondition(
                        newInnerNode,
                        mycatView.getDistribution(), condition);
                call.transformTo(new MycatSQLTableLookup(cluster, join.getTraitSet(), left, resView, joinType, join.getCondition(), correlationIds, MycatSQLTableLookup.Type.BACK));
                break;
            }
            default:
        }
    }

    private boolean isGisView(MycatView mycatView) {
        if (mycatView.getCondition().isPresent()) {
            if (mycatView.getDistribution().type() == Distribution.Type.SHARDING) {
                ShardingTable shardingTable = mycatView.getDistribution().getShardingTables().get(0);
                return !shardingTable.getIndexTables().isEmpty();
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public static void extractedTrimJoinLeftKeys(Join
                                                         join, ImmutableList.Builder<RelDataTypeField> listBuilder, Map<Integer, Integer> oldToNew) {
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
                        b0.operand(Join.class).inputs(b1 -> b1.operand(RelNode.class).anyInputs(), b1 -> b1.operand(MycatView.class).predicate(m -> ((MycatView) m).allowPushdown()).noInputs()))
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

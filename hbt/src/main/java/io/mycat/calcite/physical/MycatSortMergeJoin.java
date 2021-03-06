/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.physical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MycatSortMergeJoin extends Join implements MycatRel {
    protected MycatSortMergeJoin(RelOptCluster cluster,
                                 RelTraitSet traitSet,
                                 RelNode left,
                                 RelNode right,
                                 RexNode condition,
                                 Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    public static MycatSortMergeJoin create(
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType) {
        RelOptCluster cluster = left.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        traitSet = traitSet.replace(MycatConvention.INSTANCE);
        traitSet = traitSet.replaceIfs(
                RelCollationTraitDef.INSTANCE,
                () -> mq.collations(left));//SortMergeJoin结果也是已经排序的
        return new MycatSortMergeJoin(left.getCluster(),
                traitSet.replace(MycatConvention.INSTANCE)
                , left, right, condition, ImmutableSet.of(), joinType);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return MycatRel.explainJoin(this, "SortMergeJoin", writer);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // We assume that the inputs are sorted. The price of sorting them has
        // already been paid. The cost of the join is therefore proportional to the
        // input and output size.
        final double rightRowCount = right.estimateRowCount(mq);
        final double leftRowCount = left.estimateRowCount(mq);
        final double rowCount = mq.getRowCount(this);
        final double d = leftRowCount + rightRowCount + rowCount;
        RelOptCost relOptCost = planner.getCostFactory().makeCost(d, 0, 0);
        return relOptCost;
    }
//
//    @Override
//    public Executor implement(ExecutorImplementor implementor) {
//        return implementor.implement(this);
//    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new MycatSortMergeJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        BlockBuilder builder = new BlockBuilder();
        final Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) left, pref);
        final Expression leftExpression =
                toEnumerate(builder.append("left", leftResult.block));
        final ParameterExpression left_ =
                Expressions.parameter(leftResult.physType.getJavaRowType(), "left");
        final Result rightResult =
                implementor.visitChild(this, 1, (EnumerableRel) right, pref);
        final Expression rightExpression =
                toEnumerate(builder.append("right", rightResult.block));
        final ParameterExpression right_ =
                Expressions.parameter(rightResult.physType.getJavaRowType(), "right");
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final PhysType physType =
                PhysTypeImpl.of(typeFactory, getRowType(), pref.preferArray());
        final List<Expression> leftExpressions = new ArrayList<>();
        final List<Expression> rightExpressions = new ArrayList<>();
        for (Pair<Integer, Integer> pair : Pair.zip(joinInfo.leftKeys, joinInfo.rightKeys)) {
            final RelDataType keyType =
                    typeFactory.leastRestrictive(
                            ImmutableList.of(
                                    left.getRowType().getFieldList().get(pair.left).getType(),
                                    right.getRowType().getFieldList().get(pair.right).getType()));
            final Type keyClass = typeFactory.getJavaClass(keyType);
            leftExpressions.add(
                    EnumUtils.convert(
                            leftResult.physType.fieldReference(left_, pair.left), keyClass));
            rightExpressions.add(
                    EnumUtils.convert(
                            rightResult.physType.fieldReference(right_, pair.right), keyClass));
        }
        Expression predicate = Expressions.constant(null);
        if (!joinInfo.nonEquiConditions.isEmpty()) {
            final RexNode nonEquiCondition = RexUtil.composeConjunction(
                    getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
            if (nonEquiCondition != null) {
                predicate = EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
                        left, right, leftResult.physType, rightResult.physType, nonEquiCondition);
            }
        }
        final PhysType leftKeyPhysType =
                leftResult.physType.project(joinInfo.leftKeys, JavaRowFormat.LIST);
        final PhysType rightKeyPhysType =
                rightResult.physType.project(joinInfo.rightKeys, JavaRowFormat.LIST);

        // Generate the appropriate key Comparator (keys must be sorted in ascending order, nulls last).
        final int keysSize = joinInfo.leftKeys.size();
        final List<RelFieldCollation> fieldCollations = new ArrayList<>(keysSize);
        for (int i = 0; i < keysSize; i++) {
            fieldCollations.add(
                    new RelFieldCollation(i, RelFieldCollation.Direction.ASCENDING,
                            RelFieldCollation.NullDirection.LAST));
        }
        final RelCollation collation = RelCollations.of(fieldCollations);
        final Expression comparator = leftKeyPhysType.generateComparator(collation);

        return implementor.result(
                physType,
                builder.append(
                        Expressions.call(
                                BuiltInMethod.MERGE_JOIN.method,
                                Expressions.list(
                                        leftExpression,
                                        rightExpression,
                                        Expressions.lambda(
                                                leftKeyPhysType.record(leftExpressions), left_),
                                        Expressions.lambda(
                                                rightKeyPhysType.record(rightExpressions), right_),
                                        predicate,
                                        EnumUtils.joinSelector(joinType,
                                                physType,
                                                ImmutableList.of(
                                                        leftResult.physType, rightResult.physType)),
                                        Expressions.constant(EnumUtils.toLinq4jJoinType(joinType)),
                                        comparator))).toBlock());
    }
    @Override
    public boolean isSupportStream() {
        return true;
    }
}
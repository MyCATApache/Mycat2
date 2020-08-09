/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
package io.mycat.hbt4.executor;

import io.mycat.hbt3.MycatLookUpView;
import io.mycat.hbt3.View;
import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * complete
 */
public class MycatBatchNestedLoopJoinRule extends RelOptRule {
    public static final MycatBatchNestedLoopJoinRule INSTANCE = new MycatBatchNestedLoopJoinRule();

    int batchSize = DEFAULT_BATCH_SIZE;
    private static final int DEFAULT_BATCH_SIZE = 1000;

    public MycatBatchNestedLoopJoinRule() {
        super(operandJ(Join.class, null, join -> {
                    JoinRelType joinType = join.getJoinType();
                    boolean b = joinType == JoinRelType.INNER
                            || joinType == JoinRelType.LEFT
                            || joinType == JoinRelType.ANTI
                            || joinType == JoinRelType.SEMI;
                    if (!b) {
                        return false;
                    }
                    JoinInfo joinInfo = join.analyzeCondition();
                    return joinInfo.isEqui() && !joinInfo.leftSet().isEmpty();
                },
                operand(MycatRel.class, any()),
                operand(View.class, any())), "MycatBatchNestedLoopJoinRule");
    }


    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        JoinRelType joinType = join.getJoinType();
        return joinType == JoinRelType.INNER
                || joinType == JoinRelType.LEFT
                || joinType == JoinRelType.ANTI
                || joinType == JoinRelType.SEMI;
    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final View lookupView = call.rel(2);
        final int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        final RelOptCluster cluster = join.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RelBuilder relBuilder = call.builder();

        final Set<CorrelationId> correlationIds = new HashSet<>();
        JoinInfo joinInfo = join.analyzeCondition();

        final ArrayList<RexNode> corrVar = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            CorrelationId correlationId = cluster.createCorrel();
            correlationIds.add(correlationId);
            corrVar.add(
                    rexBuilder.makeCorrel(join.getLeft().getRowType(),
                            correlationId));
        }
        final ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();

        // Generate first condition
        RexNode condition = join.getCondition().accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef input) {
                int field = input.getIndex();
                if (field >= leftFieldCount) {
                    return rexBuilder.makeInputRef(input.getType(),
                            input.getIndex() - leftFieldCount);
                }
                requiredColumns.set(field);
                return  rexBuilder.makeFieldAccess(corrVar.get(0), field);
            }
        });
        List<RexNode> conditionList = new ArrayList<>();
        conditionList.add(condition);

        // Add batchSize-1 other conditions
        for (int i = 1; i < batchSize; i++) {
            final int corrIndex = i;
            final RexNode condition2 = condition.accept(new RexShuttle() {
                @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                    return corrVar.get(corrIndex);
                }
            });
            conditionList.add(condition2);
        }
        // Push a filter with batchSize disjunctions
        relBuilder.push(lookupView.getRelNode()).filter(relBuilder.or(conditionList));
        RelNode right = MycatLookUpView.create(relBuilder.build());

        JoinRelType joinType = join.getJoinType();
        call.transformTo(
                MycatBatchNestedLoopJoin.create(
                        convert(join.getLeft(), join.getLeft().getTraitSet()
                                .replace(MycatConvention.INSTANCE)),
                        convert(right, right.getTraitSet()
                                .replace(MycatConvention.INSTANCE)),
                        join.getCondition(),
                        requiredColumns.build(),
                        correlationIds,
                        joinType));
    }
}

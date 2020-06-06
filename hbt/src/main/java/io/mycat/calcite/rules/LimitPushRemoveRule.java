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
package io.mycat.calcite.rules;

import com.google.common.base.Predicate;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * chenjunwen
 * <p>
 * EXPLAIN SELECT * FROM db1.travelrecord  LIMIT 500 ,10000;
 */
public class LimitPushRemoveRule extends RelOptRule {
   public static final LimitPushRemoveRule INSTANCE = new LimitPushRemoveRule();
    private static final Logger LOGGER = LoggerFactory.getLogger(LimitPushRemoveRule.class);

    public LimitPushRemoveRule() {
        super(
                operandJ(Sort.class, null, (Predicate<Sort>) LimitPushRemoveRule::sortApply,
                        operandJ(Union.class, null, (Predicate<Union>) LimitPushRemoveRule::unionApply, any())),
                RelFactories.LOGICAL_BUILDER, "LimitRemoveRule");
    }

    private static boolean unionApply(@Nullable Union union) {
        if (union == null || union.isDistinct()) {
            return false;
        }
        return !(union.getInput(0) instanceof Sort) && !(union.getInput(1) instanceof Sort);
    }

    private static boolean sortApply(Sort call) {
        if (call != null) {
            if (call.isDistinct() ||
                    (call.getChildExps() == null)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {


        final Sort sort = call.rel(0);
        if (!sortApply(sort)) {
            return;
        }

        final Union union = call.rel(1);
        if (!unionApply(union)) {
            return;
        }
        RelBuilder builder = call.builder();

        builder.clear();
        if (!union.isDistinct()) {
            ArrayList<RelNode> newNodes = new ArrayList<>();
            if (sort.offset == null && sort.fetch != null) {
                for (RelNode input : union.getInputs()) {
                    Sort pushSort = sort.copy(sort.getTraitSet(), input, sort.getCollation(), null, sort.fetch);
                    newNodes.add(pushSort);
                }
            } else if (sort.offset != null && sort.fetch != null) {
                for (RelNode input : union.getInputs()) {
                    if (sort.offset instanceof RexLiteral && sort.fetch instanceof RexLiteral) {
                        Comparable start = ((RexLiteral) sort.offset).getValue();
                        Comparable end = ((RexLiteral) sort.fetch).getValue();
                        if (start instanceof Number && end instanceof Number) {
                            Sort pushSort = sort.copy(
                                    sort.getTraitSet(),
                                    input,
                                    sort.getCollation(),
                                    builder.literal(0),
                                    builder.literal(
                                            BigDecimal.valueOf(((Number) start).longValue())
                                                    .add(
                                                            BigDecimal.valueOf(((Number) end).longValue()))
                                    )
                            );
                            newNodes.add(pushSort);
                            continue;
                        }
                    }
                }
            }
            if (!newNodes.isEmpty()) {
                builder.pushAll(newNodes);
                RelNode build = builder.union(true, newNodes.size()).build();
                Sort copy = sort.copy(sort.getTraitSet(), build, sort.getCollation());
                call.transformTo(copy);
            }
        }

    }
}

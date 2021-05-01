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

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatMergeSort;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MycatExtraSortRule extends RelOptRule {

    public static final MycatExtraSortRule INSTANCE = new MycatExtraSortRule();

    public MycatExtraSortRule() {
        super(operand(Join.class, operand(MycatView.class, none()), operand(MycatView.class, none())), "MycatExtraSortRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        MycatView leftMycatView = call.rel(1);
        MycatView rightMycatView = call.rel(2);

        final JoinInfo info = join.analyzeCondition();
        if (!EnumerableMergeJoin.isMergeJoinSupported(join.getJoinType())) {
            // EnumerableMergeJoin only supports certain join types.
            return;
        }
        if (info.pairs().isEmpty()) {
            // EnumerableMergeJoin CAN support cartesian join, but disable it for now.
            return;
        }
        final List<RelCollation> collations = new ArrayList<>();
        for (Ord<MycatView> ord : Ord.zip(ImmutableList.of(leftMycatView, rightMycatView))) {
            final List<RelFieldCollation> fieldCollations = new ArrayList<>();
            for (int key : info.keys().get(ord.i)) {
                fieldCollations.add(
                        new RelFieldCollation(key, RelFieldCollation.Direction.ASCENDING,
                                RelFieldCollation.NullDirection.LAST));
            }
            collations.add(RelCollations.of(fieldCollations));
        }
        call.transformTo(join.copy(join.getTraitSet(), ImmutableList.of(
                pushSort(leftMycatView, collations.get(0)), pushSort(rightMycatView, collations.get(1)))));
        return;
    }

    private RelNode pushSort(RelNode relNode, RelCollation relCollation) {
        RelCollation trait = relNode.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        if (trait.equals(relCollation)){
            return relNode;
        }
        RelBuilder relBuilder = relBuilderFactory.create(relNode.getCluster(), null);
        if (relNode instanceof MycatView) {
            MycatView mycatView = (MycatView) relNode;
            RelNode innerRelNode = mycatView.getRelNode();
            relNode = mycatView.changeTo(relBuilder.push(innerRelNode).sort(relCollation).build());
            return MycatMergeSort.create(relNode.getTraitSet().replace(relCollation), relNode, relCollation,
                    null,
                    null);
        } else {
            return relBuilder.push(relNode).sort(relCollation).build();
        }
    }
}

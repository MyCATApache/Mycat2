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
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatMergeSort;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MycatRBORule extends RelRule<MycatRBORule.Config> {


    public MycatRBORule(MycatRBORule.Config config) {
        super((Config) config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode join = call.rel(0);
//        call.getChildRels()

    }

    public interface Config extends RelRule.Config {
        MycatRBORule.Config BOTTOM_VIEW = EMPTY
                .as(MycatRBORule.Config.class)
                .withOperandFor(MycatView.class);
        MycatRBORule.Config BOTTOM_TABLESCAN = EMPTY
                .as(MycatRBORule.Config.class)
                .withOperandFor(LogicalTableScan.class);

        @Override default MycatRBORule toRule() {
            return new MycatRBORule(this);
        }

        /** Defines an operand tree for the given classes. */
        default MycatRBORule.Config withOperandFor(Class<? extends RelNode> inputClass) {
            return withOperandSupplier(b0 ->
                    b0.operand(RelNode.class).predicate(r->!(r instanceof MycatView)).unorderedInputs(b1 ->
                            b1.operand(inputClass).anyInputs()))
                    .as(MycatRBORule.Config.class);
        }
    }
}

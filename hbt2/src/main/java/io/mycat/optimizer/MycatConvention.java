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
package io.mycat.optimizer;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;

public class MycatConvention extends Convention.Impl {

  public static final MycatConvention INSTANCE = new MycatConvention();

  public static final double COST_MULTIPLIER = 0.8d;

  public MycatConvention() {
    super("MYCAT2", MycatRel.class);
  }

  @Override public void register(RelOptPlanner planner) {
    for (RelOptRule rule : MycatRules.rules(this)) {
      planner.addRule(rule);
    }
    planner.addRule(FilterSetOpTransposeRule.INSTANCE);
    planner.addRule(ProjectRemoveRule.INSTANCE);
  }
}

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
package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.spm.ParamHolder;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.mycat.calcite.MycatRexCompiler.rexBuilder;

public class MycatRexExecutor implements RexExecutor {
    public static final MycatRexExecutor INSTANCE = new MycatRexExecutor();
    public static final Logger LOGGER = LoggerFactory.getLogger(MycatRexExecutor.class);
    public static final RexShuttle PARAM_RESOLVER = new RexShuttle() {
        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            List<Object> params = ParamHolder.CURRENT_THREAD_LOCAL.get();
            if (params != null) {
                Object o = params.get(dynamicParam.getIndex());
                return rexBuilder.makeLiteral(o, dynamicParam.getType(), true);
            }
            return super.visitDynamicParam(dynamicParam);
        }
    };

    @Override
    public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
        try {
            for (RexNode constExp : constExps) {
                constExp = constExp.accept(PARAM_RESOLVER);
                RexNode rexNode = RexUtil.toDnf(rexBuilder, constExp);
                RexNode res;
                if (rexNode.getKind() == SqlKind.OR) {
                    res = reduce(rexBuilder, constExp, rexNode);
                } else {
                    res = constExp;
                }
                reducedValues.add(res);
            }
        } catch (Throwable throwable) {
            LOGGER.warn("", throwable);
            reducedValues.clear();
            reducedValues.addAll(constExps);
        }
    }

    private RexNode reduce(RexBuilder rexBuilder, RexNode constExp, RexNode rexNode) {
        RexNode res;
        List<RexNode> disjunctions = RelOptUtil.disjunctions(rexNode);
        List<RexNode> rexNodes = new ArrayList<>(disjunctions);
        for (RexNode node : ImmutableList.copyOf(rexNodes)) {
            if (node.getKind() == SqlKind.AND) {
                List<RexNode> ands = ((RexCall) node).getOperands();
                Set<RexNode> equals = new HashSet<>();
                for (RexNode and : ands) {
                    if (and.getKind() == SqlKind.EQUALS) {
                        equals.add(and);
                    }
                }
                if (equals.size() > 1) {
                    rexNodes.remove(node);
                }
            }
        }
        if (disjunctions.size() == rexNodes.size()) {
            res = constExp;
        } else {
            res = RexUtil.composeConjunction(rexBuilder, rexNodes);
        }
        return res;
    }
}
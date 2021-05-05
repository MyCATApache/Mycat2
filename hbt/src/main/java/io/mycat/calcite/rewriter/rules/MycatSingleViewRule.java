/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.calcite.rewriter.rules;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.SQLRBORewriter;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public abstract class MycatSingleViewRule extends RelRule<MycatSingleViewRule.Config> {

    public MycatSingleViewRule(Config config) {
        super(config);
    }

    public MycatSingleViewRule(Class<? extends SingleRel> up) {
        super(Config.EMPTY.as(MycatSingleViewRule.Config.class)
                .withOperandFor(up));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SQLRBORewriter.on(call.rel(1), call.rel(0)).ifPresent(c->{
            call.transformTo(c);
        });
    }

    public interface Config extends RelRule.Config {
        default MycatSingleViewRule.Config withOperandFor(Class<? extends SingleRel> up) {
            return withOperandSupplier(b0 ->
                    b0.operand(up).oneInput(b1 -> b1.operand(MycatView.class).noInputs()))
                    .withDescription("MycatSingleViewRule_" + up.getName())
                    .as(MycatSingleViewRule.Config.class);
        }
    }

    public static final List<RelRule> RULES = Arrays.asList(
            new MycatSingleViewRule((Filter.class)) {
            },
            new MycatSingleViewRule((Project.class)) {
            },
            new MycatSingleViewRule((Aggregate.class)) {

            },
            new MycatSingleViewRule((Sort.class)) {

            },
            new MycatSingleViewRule((Window.class)) {

            },
            new MycatSingleViewRule((Calc.class)) {

            }
    );
}

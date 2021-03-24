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
package io.mycat.calcite.rules;

import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.RelMdSqlViews;
import io.mycat.calcite.rewriter.SQLRBORewriter;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;

public class MycatSortViewRule extends RelOptRule {
    public final static MycatSortViewRule INSTANCE = new MycatSortViewRule();

    public MycatSortViewRule() {
        super(operand(LogicalSort.class,
                operand(MycatView.class, none())), "MycatFilterViewRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort up = call.rel(0);
        RelNode view = call.rel(1);
        if (RelMdSqlViews.sort(up)) {
            RelNode res = SQLRBORewriter.sort(view,up);
            if (res != null) {
                call.transformTo(res);
            }
        }
    }
}
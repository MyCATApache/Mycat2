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
package io.mycat.rsqlBuilder;

import io.mycat.describer.CallExpr;
import io.mycat.describer.ParenthesesExpr;
import io.mycat.describer.ParseNode;

import java.util.ArrayList;
import java.util.List;

public class DotCallResolver extends CopyNodeVisitor {

    public DotCallResolver() {
    }

    @Override
    public void visit(CallExpr call) {
        String name = call.getName();
        List<ParseNode> args = call.getArgs().getExprs();
        if ("DOT".equalsIgnoreCase(name) && args.size() == 2) {
            ParseNode m = args.get(1);
            if (m instanceof CallExpr) {
                List<ParseNode> exprs1 = ((CallExpr) m).getArgs().getExprs();
                ArrayList<ParseNode> objects = new ArrayList<>();
                objects.add(0, args.get(0));
                objects.addAll(exprs1);
                CallExpr callExpr = new CallExpr(((CallExpr) m).getName(), new ParenthesesExpr(objects));
                callExpr.accept(this);
                return;
            }
        }
        super.visit(call);
    }

    @Override
    public void endVisit(CallExpr call) {
        List<ParseNode> exprs = call.getArgs().getExprs();
        if ("DOT".equals(call.getName().toUpperCase())) {
            if (exprs.size() == 2 && (exprs.get(1) instanceof CallExpr)) {
                return;
            }
        }
        super.endVisit(call);
    }

}
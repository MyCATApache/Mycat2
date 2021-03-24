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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;

/**
 * Visitor for checking whether part of projection is a user defined function or not
 */
public class CheckingUserDefinedFunctionVisitor extends RexVisitorImpl<Void> {

    private boolean containsUsedDefinedFunction = false;

   public CheckingUserDefinedFunctionVisitor() {
        super(true);
    }

    public boolean containsUserDefinedFunction() {
        return containsUsedDefinedFunction;
    }

    @Override
    public Void visitCall(RexCall call) {
        SqlOperator operator = call.getOperator();
        if (operator instanceof SqlFunction
                && ((SqlFunction) operator).getFunctionType().isUserDefined()) {
            containsUsedDefinedFunction |= true;
        }
        return super.visitCall(call);
    }

}
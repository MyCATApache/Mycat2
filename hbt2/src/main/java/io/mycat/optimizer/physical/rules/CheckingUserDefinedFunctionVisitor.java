package io.mycat.optimizer.physical.rules;

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
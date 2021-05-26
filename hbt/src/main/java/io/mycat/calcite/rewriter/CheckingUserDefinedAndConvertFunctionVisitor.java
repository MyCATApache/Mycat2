package io.mycat.calcite.rewriter;

import io.mycat.calcite.sqlfunction.infofunction.MycatSessionValueFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;

import static io.mycat.calcite.rewriter.SQLRBORewriter.Information_Functions;

public class CheckingUserDefinedAndConvertFunctionVisitor
        extends RexVisitorImpl<Void> {

    private boolean containsUsedDefinedFunction = false;

    CheckingUserDefinedAndConvertFunctionVisitor() {
        super(true);
    }

    public boolean containsUserDefinedFunction() {
        return containsUsedDefinedFunction;
    }

    @Override
    public Void visitCall(RexCall call) {
        SqlOperator operator = call.getOperator();
        String name = operator.getName();
        if (operator instanceof SqlFunction) {
            containsUsedDefinedFunction |= Information_Functions.containsKey(name, false);
        }
        if (operator == MycatSessionValueFunction.INSTANCE) {
            containsUsedDefinedFunction = true;
        }
        return super.visitCall(call);
    }

    @Override
    public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
        containsUsedDefinedFunction = true;
        return super.visitCorrelVariable(correlVariable);
    }
}
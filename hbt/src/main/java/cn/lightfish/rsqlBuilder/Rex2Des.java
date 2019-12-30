package cn.lightfish.rsqlBuilder;

import org.apache.calcite.rex.*;

public class Rex2Des implements RexBiVisitor {
    @Override
    public Object visitInputRef(RexInputRef inputRef, Object arg) {
        return null;
    }

    @Override
    public Object visitLocalRef(RexLocalRef localRef, Object arg) {
        return null;
    }

    @Override
    public Object visitLiteral(RexLiteral literal, Object arg) {
        return null;
    }

    @Override
    public Object visitCall(RexCall call, Object arg) {
        return null;
    }

    @Override
    public Object visitOver(RexOver over, Object arg) {
        return null;
    }

    @Override
    public Object visitCorrelVariable(RexCorrelVariable correlVariable, Object arg) {
        return null;
    }

    @Override
    public Object visitDynamicParam(RexDynamicParam dynamicParam, Object arg) {
        return null;
    }

    @Override
    public Object visitRangeRef(RexRangeRef rangeRef, Object arg) {
        return null;
    }

    @Override
    public Object visitFieldAccess(RexFieldAccess fieldAccess, Object arg) {
        return null;
    }

    @Override
    public Object visitSubQuery(RexSubQuery subQuery, Object arg) {
        return null;
    }

    @Override
    public Object visitTableInputRef(RexTableInputRef ref, Object arg) {
        return null;
    }

    @Override
    public Object visitPatternFieldRef(RexPatternFieldRef ref, Object arg) {
        return null;
    }
}
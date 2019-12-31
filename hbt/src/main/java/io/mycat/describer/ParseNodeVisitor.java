package io.mycat.describer;

import cn.lightfish.describer.literal.*;
import io.mycat.describer.literal.*;

public interface ParseNodeVisitor {

    void visit(Bind bind);
    void endVisit(Bind bind);

    void visit(CallExpr call);

    void endVisit(CallExpr call);

    void visit(IdLiteral id);

    void endVisit(IdLiteral id);

    void visit(ParenthesesExpr parenthesesExpr);

    void endVisit(ParenthesesExpr parenthesesExpr);

    void visit(IntegerLiteral numberLiteral);

    void endVisit(IntegerLiteral numberLiteral);

    void visit(StringLiteral stringLiteral);

    void endVisit(StringLiteral stringLiteral);

    void visit(DecimalLiteral decimalLiteral);

    void endVisit(DecimalLiteral decimalLiteral);

    void visit(PropertyLiteral propertyLiteral);

    void endVisit(PropertyLiteral propertyLiteral);
}
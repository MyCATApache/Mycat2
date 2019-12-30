package cn.lightfish.describer;

import cn.lightfish.describer.literal.*;

import java.util.List;

public class NodeVisitorImpl implements ParseNodeVisitor {
    final StringBuilder sb = new StringBuilder();

    @Override
    public void visit(Bind bind) {

    }

    @Override
    public void endVisit(Bind bind) {

    }

    @Override
    public void visit(CallExpr call) {
        sb.append(call.getName());
        call.getArgs().accept(this);
    }

    @Override
    public void endVisit(CallExpr call) {

    }

    @Override
    public void visit(IdLiteral id) {
        //sb.append("new Identifier(\"").append(id.getId()).append("\")");
        sb.append("id(\"").append(id.getId()).append("\")");
    }

    @Override
    public void endVisit(IdLiteral id) {

    }

    @Override
    public void visit(ParenthesesExpr parenthesesExpr) {
        sb.append("(");
        List<ParseNode> exprs = parenthesesExpr.getExprs();
        if (!exprs.isEmpty()) {
            for (ParseNode expr : exprs.subList(0, exprs.size() - 1)) {
                expr.accept(this);
                sb.append(",");
            }
            exprs.get(exprs.size() - 1).accept(this);
        }
        sb.append(")");
    }

    @Override
    public void endVisit(ParenthesesExpr parenthesesExpr) {

    }

    @Override
    public void visit(IntegerLiteral numberLiteral) {
//        sb.append("new Literal(BigInteger.valueOf(").append(numberLiteral.getNumber()).append("))");
        sb.append("literal(").append(numberLiteral.getNumber()).append(")");
    }

    @Override
    public void endVisit(IntegerLiteral numberLiteral) {

    }

    @Override
    public void visit(StringLiteral stringLiteral) {
        sb.append("literal(\"").append(stringLiteral.getString()).append("\")");
    }

    @Override
    public void endVisit(StringLiteral stringLiteral) {

    }

    @Override
    public void visit(DecimalLiteral decimalLiteral) {
        sb.append("literal(").append(decimalLiteral.getNumber().toString()).append(")");
    }

    @Override
    public void endVisit(DecimalLiteral decimalLiteral) {

    }

    @Override
    public void visit(PropertyLiteral propertyLiteral) {
        sb.append("property(");
        sb.append(String.join(",", "\"" + propertyLiteral.getValue() + "\""));
        sb.append("))");
    }

    @Override
    public void endVisit(PropertyLiteral propertyLiteral) {

    }


    public String getText() {
        return sb.toString();
    }
}
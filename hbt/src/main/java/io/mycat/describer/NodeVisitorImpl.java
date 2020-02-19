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
package io.mycat.describer;

import io.mycat.describer.literal.*;

import java.util.List;

/**
 * @author jamie12221
 **/
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
    public void visit(BooleanLiteral booleanLiteral) {
        sb.append("literal(").append(booleanLiteral.getValue().toString()).append(")");
    }

    @Override
    public void endVisit(BooleanLiteral booleanLiteral) {

    }


    public String getText() {
        return sb.toString();
    }
}
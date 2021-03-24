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
package io.mycat.hbt.parser;


import io.mycat.hbt.parser.literal.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author jamie12221
 **/
public class CopyNodeVisitor implements ParseNodeVisitor {

    protected final LinkedList<ParseNode> stack = new LinkedList<>();
    protected Bind res;

    public CopyNodeVisitor() {
    }

    @Override
    public void visit(Bind bind) {
        bind.getExpr().accept(this);
    }

    @Override
    public void endVisit(Bind bind) {
        res = new Bind(bind.getName(), stack.pop());
    }

    @Override
    public void visit(CallExpr call) {
        String name = call.getName();
        List<ParseNode> args = call.getArgs().getExprs();
        for (ParseNode c : args) {
            c.accept(this);
        }
    }

    @Override
    public void endVisit(CallExpr call) {
        List<ParseNode> exprs = call.getArgs().getExprs();
        int size = exprs.size();
        ArrayList<ParseNode> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(stack.pop());
        }
        Collections.reverse(list);
        stack.push(new CallExpr(call.getName(), new ParenthesesExpr(list)));
    }

    @Override
    public void visit(IdLiteral id) {

    }

    @Override
    public void endVisit(IdLiteral id) {
        stack.push(id);
    }

    @Override
    public void visit(ParenthesesExpr parenthesesExpr) {
        List<ParseNode> exprs = parenthesesExpr.getExprs();
        for (ParseNode expr : exprs) {
            expr.accept(this);
        }
    }

    @Override
    public void endVisit(ParenthesesExpr parenthesesExpr) {
        int size = parenthesesExpr.getExprs().size();
        List<ParseNode> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(stack.pop());
        }
        Collections.reverse(list);
        stack.push(new ParenthesesExpr(list));
    }

    @Override
    public void visit(IntegerLiteral numberLiteral) {
        stack.push(numberLiteral);
    }

    @Override
    public void endVisit(IntegerLiteral numberLiteral) {

    }

    @Override
    public void visit(StringLiteral stringLiteral) {

    }

    @Override
    public void endVisit(StringLiteral stringLiteral) {
        stack.push(stringLiteral);
    }

    @Override
    public void visit(DecimalLiteral decimalLiteral) {

    }

    @Override
    public void endVisit(DecimalLiteral decimalLiteral) {
        stack.push(decimalLiteral);
    }

    @Override
    public void visit(BooleanLiteral booleanLiteral) {

    }

    @Override
    public void endVisit(BooleanLiteral booleanLiteral) {
        stack.push(booleanLiteral);
    }

    @Override
    public void visit(NullLiteral nullLiteral) {

    }

    @Override
    public void endVisit(NullLiteral nullLiteral) {
        stack.push(nullLiteral);
    }

    @Override
    public void visit(ParamLiteral paramLiteral) {

    }

    @Override
    public void endVisit(ParamLiteral paramLiteral) {
        stack.push(paramLiteral);
    }


    public <T> T getStack() {
        return (T) stack.peek();
    }

    public Bind getRes() {
        return res;
    }
}
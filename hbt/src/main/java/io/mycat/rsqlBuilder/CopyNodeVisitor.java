package io.mycat.rsqlBuilder;

import io.mycat.describer.*;
import io.mycat.describer.literal.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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
    public void visit(PropertyLiteral propertyLiteral) {

    }

    @Override
    public void endVisit(PropertyLiteral propertyLiteral) {
        stack.push(propertyLiteral.copy());
    }

    public <T> T getStack() {
        return (T) stack.peek();
    }

    public Bind getRes() {
        return res;
    }
}
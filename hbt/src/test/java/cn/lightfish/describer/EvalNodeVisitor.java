package cn.lightfish.describer;

import cn.lightfish.describer.literal.*;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.*;

public class EvalNodeVisitor implements ParseNodeVisitor {
    Map<FunctionSig, Builder> map = new HashMap<>();

    ArrayDeque<ParseNode> stack = new ArrayDeque<>();

    public EvalNodeVisitor() {
        map.put(new FunctionSig("+", "(int,int)"), (exprs) -> {
            IntegerLiteral one = cast(exprs.get(0));
            IntegerLiteral two = cast(exprs.get(1));
            return new IntegerLiteral(one.getNumber().add(two.getNumber()));
        });
    }

    private <T> T cast(ParseNode node) {
        return (T) (node);
    }

    @Override
    public void visit(Bind bind) {
        bind.expr.accept(this);
    }

    @Override
    public void endVisit(Bind bind) {
        bind.expr = stack.pop();
    }

    @Override
    public void visit(CallExpr call) {
        call.getArgs().accept(this);
    }

    @Override
    public void endVisit(CallExpr call) {
        String name = call.getName();
        String type = getType();
        FunctionSig functionSig = new FunctionSig(name, type);
        Builder function = map.get(functionSig);
        ParenthesesExpr pop = (ParenthesesExpr) stack.pop();
        stack.push(function.eval(pop.getExprs()));
    }

    public String getType() {
        ParseNode peek = stack.peek();
        return getType(peek);
    }

    private String getType(ParseNode peek) {
        if (peek == null) {
            return "()";
        } else if (peek instanceof ParenthesesExpr) {
            ParenthesesExpr peek1 = (ParenthesesExpr) peek;
            StringBuilder sb = new StringBuilder("(");
            List<ParseNode> exprs = peek1.getExprs();
            for (int i = 0; i < exprs.size(); i++) {
                ParseNode expr = exprs.get(i);
                if (expr instanceof StringLiteral) {
                    sb.append("str");
                } else if (expr instanceof IntegerLiteral) {
                    sb.append("int");
                } else if (expr instanceof DecimalLiteral) {
                    sb.append("double");
                } else {
                    sb.append(expr.getClass().getSimpleName());
                }
                if (i != exprs.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append(")");
            return sb.toString();
        } else if (peek instanceof IdLiteral) {
            return "id";
        } else {
            throw new RuntimeException();
        }
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


    }

    @Override
    public void endVisit(ParenthesesExpr parenthesesExpr) {
        List<ParseNode> exprs = parenthesesExpr.getExprs();
        for (ParseNode expr : exprs) {
            expr.accept(this);
        }
        ArrayList<ParseNode> list = new ArrayList<>(exprs.size());
        for (int i = 0; i < exprs.size(); i++) {
            list.add(stack.pop());
        }
        stack.push(new ParenthesesExpr(list));
    }

    @Override
    public void visit(IntegerLiteral numberLiteral) {

    }

    @Override
    public void endVisit(IntegerLiteral numberLiteral) {
        stack.push(numberLiteral);
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
        stack.push(propertyLiteral.copy());
    }

    @Override
    public void endVisit(PropertyLiteral propertyLiteral) {

    }

    @EqualsAndHashCode
    @AllArgsConstructor
    @Getter
    static class FunctionSig {
        String name;
        String type;
    }
}
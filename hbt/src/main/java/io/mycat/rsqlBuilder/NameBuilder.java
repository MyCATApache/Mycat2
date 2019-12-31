package io.mycat.rsqlBuilder;

import io.mycat.describer.CallExpr;
import io.mycat.describer.ParseNode;
import io.mycat.describer.ParenthesesExpr;
import io.mycat.describer.literal.IdLiteral;
import io.mycat.describer.literal.PropertyLiteral;
import io.mycat.rsqlBuilder.schema.SchemaMatcher;

import java.util.*;

public class NameBuilder extends CopyNodeVisitor {

    private final SchemaMatcher schemaMatcher;
    private final Map<String, ParseNode> variables;

    public NameBuilder() {
        this(null, Collections.emptyMap());
    }

    public NameBuilder(SchemaMatcher schemaMatcher) {
        this(schemaMatcher, Collections.emptyMap());
    }

    public NameBuilder(SchemaMatcher schemaMatcher, Map<String, ParseNode> variables) {
        this.schemaMatcher = schemaMatcher;
        this.variables = variables;
    }

    @Override
    public void endVisit(CallExpr call) {
        List<ParseNode> exprs = call.getArgs().getExprs();
        switch (call.getName().toUpperCase()) {
            case "DOT": {
                dot(exprs);
                return;
            }
            default: {
                int size = exprs.size();
                ArrayDeque list = new ArrayDeque(size);
                for (int i = 0; i < size; i++) {
                    list.push(stack.pop());
                }
                stack.push(new CallExpr(call.getName(), new ParenthesesExpr(new ArrayList<>(list))));
            }
        }
    }

    private void dot(List<ParseNode> exprs) {
        if (exprs.size() == 2) {
            ParseNode second = stack.pop();
            ParseNode pop = stack.pop();


            if (pop instanceof DotAble) {
                DotAble first = (DotAble) pop;
                stack.push(first.dot(((IdLiteral) second).getId()));
                return;
            }

            if (pop instanceof IdLiteral) {
                ParseNode var = getVariables(((IdLiteral) pop).getId());
                if (var instanceof DotAble) {
                    IdLiteral second1 = (IdLiteral) second;
                    stack.push(((DotAble) var).dot(second1.getId()));
                    return;
                }
            }
            if (second instanceof IdLiteral && pop instanceof IdLiteral) {
                stack.push(new PropertyLiteral(Arrays.asList(((IdLiteral) pop).getId(), ((IdLiteral) second).getId())));
                return;
            }
            if (second instanceof IdLiteral && pop instanceof PropertyLiteral) {
                List<String> value = ((PropertyLiteral) pop).getValue();
                ArrayList<String> v = new ArrayList<>(value.size() + 1);
                v.addAll(value);
                v.add(((IdLiteral) second).getId());
                stack.push(new PropertyLiteral(v));
                return;
            }
            throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void endVisit(IdLiteral id) {
        stack.push(resloveName(id));
    }

    public <T extends ParseNode> T getVariables(String pop) {
        return (T) variables.get(pop);
    }

    public ParseNode resloveName(IdLiteral name) {
        if (schemaMatcher != null) {
            ParseNode schemaObject = schemaMatcher.getSchemaObject(name);
            if (schemaObject != null) {
                return schemaObject;
            }
        }
        ParseNode o = variables.get(name.getId());
        if (o != null) {
            return o;
        } else {
            return name;
        }
    }
}
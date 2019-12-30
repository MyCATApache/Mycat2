package cn.lightfish.describer;

import lombok.Getter;

import java.text.MessageFormat;
import java.util.Objects;

@Getter
public class Bind implements ParseNode {
    String name;
    ParseNode expr;

    public Bind(String name, ParseNode expr) {
        this.name = name;
        this.expr = expr;
    }


    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public Bind copy() {
        return new Bind(name, expr.copy());
    }

    @Override
    public String toString() {
        return MessageFormat.format( "let {0} = {1};",Objects.toString(name), Objects.toString(expr));
    }
}
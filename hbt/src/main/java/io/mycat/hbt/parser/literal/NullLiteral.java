package io.mycat.hbt.parser.literal;

import io.mycat.hbt.parser.ParseNodeVisitor;

import java.util.Objects;

public class NullLiteral implements Literal {

    public NullLiteral() {

    }

    @Override
    public String toString() {
        return Objects.toString(null);
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public NullLiteral copy() {
        return this;
    }
}
package io.mycat.hbt.parser.literal;

import io.mycat.hbt.parser.ParseNodeVisitor;
import lombok.Getter;

@Getter
public class BooleanLiteral implements Literal {
    private final Boolean value;

    public BooleanLiteral(Boolean value) {
        this.value = value;
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public BooleanLiteral copy() {
        return this;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
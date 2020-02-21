package io.mycat.describer.literal;

import io.mycat.describer.ParseNodeVisitor;
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
}
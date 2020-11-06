package io.mycat.hbt.parser.literal;

import io.mycat.hbt.parser.ParseNodeVisitor;

public class ParamLiteral  implements Literal {

    public ParamLiteral() {
    }

    @Override
    public String toString() {
        return "?";
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public ParamLiteral copy() {
        return this;
    }
}
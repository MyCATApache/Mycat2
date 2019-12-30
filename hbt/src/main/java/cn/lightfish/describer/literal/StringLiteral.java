package cn.lightfish.describer.literal;

import cn.lightfish.describer.ParseNodeVisitor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

@EqualsAndHashCode
@Getter
public class StringLiteral implements Literal {
    final String string;

    public StringLiteral(String string) {
        this.string = string;
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public StringLiteral copy() {
        return this;
    }

    @Override
    public String toString() {
        return Objects.toString(string);
    }
}
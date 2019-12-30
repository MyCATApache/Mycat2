package cn.lightfish.describer.literal;

import cn.lightfish.describer.ParseNodeVisitor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

@EqualsAndHashCode
@Getter
public class IdLiteral implements Literal {
    final String id;

    public IdLiteral(String id) {
        this.id = id;
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public IdLiteral copy() {
        return this;
    }

    @Override
    public String toString() {
        return Objects.toString(id);
    }
}
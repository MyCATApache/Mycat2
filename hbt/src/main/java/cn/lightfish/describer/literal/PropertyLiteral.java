package cn.lightfish.describer.literal;

import cn.lightfish.describer.ParseNodeVisitor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode
@Getter
public class PropertyLiteral implements Literal {
    List<String> value;

    public PropertyLiteral(List<String> value) {
        this.value = new ArrayList<>(value);
    }

    @Override
    public String toString() {
        return String.join(".", value);
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public PropertyLiteral copy() {
        return new PropertyLiteral(value);
    }
}
package cn.lightfish.describer.literal;

import cn.lightfish.describer.ParseNodeVisitor;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;
import java.util.Objects;

@EqualsAndHashCode
public class DecimalLiteral implements Literal {
    private final BigDecimal number;

    public DecimalLiteral(BigDecimal number) {
        this.number = number;
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public DecimalLiteral copy() {
        return this;
    }

    @Override
    public String toString() {
        return Objects.toString(number);
    }

    public BigDecimal getNumber() {
        return number;
    }
}
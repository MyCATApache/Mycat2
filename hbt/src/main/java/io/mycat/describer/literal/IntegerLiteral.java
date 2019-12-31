package io.mycat.describer.literal;

import io.mycat.describer.ParseNodeVisitor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.math.BigInteger;
import java.util.Objects;

@EqualsAndHashCode
@Getter
public class IntegerLiteral implements Literal {
    private BigInteger number;

    public IntegerLiteral(BigInteger number) {
        this.number = number;
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public IntegerLiteral copy() {
        return this;
    }

    @Override
    public String toString() {
        return Objects.toString(number);
    }
}
package cn.lightfish.wu.ast.base;

import cn.lightfish.wu.Op;
import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;

@Data
public class Literal extends Expr {
    final Object value;

    public Literal(Object value) {
        super(Op.LITERAL);
        if (value instanceof Double) {
            this.value = BigDecimal.valueOf((Double) value);
        } else if (value instanceof Float) {
            this.value = BigDecimal.valueOf((Float) value);
        } else if (value instanceof Integer) {
            this.value = BigInteger.valueOf((Integer) value);
        } else if (value instanceof Long) {
            this.value = BigInteger.valueOf((Long) value);
        } else {
            this.value = value;
        }
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}

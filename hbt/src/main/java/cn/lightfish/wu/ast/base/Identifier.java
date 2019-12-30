package cn.lightfish.wu.ast.base;

import cn.lightfish.wu.Op;
import lombok.Data;

import java.util.Objects;

@Data
public class Identifier extends Expr {
    final String value;

    public Identifier(String value) {
        super(Op.IDENTIFIER);
        this.value = value;
    }

    public boolean isStar() {
        return "*".equalsIgnoreCase(Objects.toString(value));
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}



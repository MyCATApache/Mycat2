package io.mycat.hbt.ast.base;

import io.mycat.hbt.Op;
import lombok.Data;

import java.util.List;

@Data
public class Property extends Expr {
    final List<String> value;

    public Property(List<String> value) {
        super(Op.PROPERTY);
        this.value = value;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
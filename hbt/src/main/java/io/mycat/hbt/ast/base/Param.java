package io.mycat.hbt.ast.base;

import io.mycat.hbt.ast.HBTOp;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class Param extends Expr {
    public Param() {
        super(HBTOp.PARAM);
    }

    @Override
    public String toString() {
        return "?";
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
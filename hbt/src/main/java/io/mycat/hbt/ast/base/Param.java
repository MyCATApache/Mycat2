package io.mycat.hbt.ast.base;

import io.mycat.hbt.HBTOp;

public class Param extends Expr {
    public Param() {
        super(HBTOp.PARAM);
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
package io.mycat.hbt.ast.base;

import io.mycat.hbt.Op;

public class Param extends Node {
    public Param() {
        super(Op.PARAM);
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
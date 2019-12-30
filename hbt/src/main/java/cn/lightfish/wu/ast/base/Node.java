package cn.lightfish.wu.ast.base;

import cn.lightfish.wu.Op;

public abstract class Node {
    public final Op op;

    public Node(Op op) {
        this.op = op;
    }

    public Op getOp() {
        return op;
    }

    abstract public void accept(NodeVisitor visitor);
}
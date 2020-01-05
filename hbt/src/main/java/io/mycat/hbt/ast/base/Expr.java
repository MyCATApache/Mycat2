package io.mycat.hbt.ast.base;

import io.mycat.hbt.Op;
import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class Expr extends Node {
    List<Expr> nodes;

    public Expr(Op op, List<Expr> nodes) {
        super(op);
        this.nodes = nodes;
    }

    public Expr(Op op, Expr... nodes) {
        this(op, Arrays.asList(nodes));
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return op.getFun() + "(" +
                nodes.stream().map(i -> i.toString()).collect(Collectors.joining(",")) +
                ')';
    }
}
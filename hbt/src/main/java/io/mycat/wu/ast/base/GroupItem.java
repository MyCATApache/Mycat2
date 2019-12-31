package io.mycat.wu.ast.base;

import io.mycat.wu.Op;
import lombok.Data;

import java.util.List;

@Data
public class GroupItem extends Node {
    private final List<Expr> exprs;

    public GroupItem(Op op, List<Expr> exprs) {
        super(op);
        this.exprs = exprs;
    }

    @Override
    public void accept(NodeVisitor visitor) {

    }
}
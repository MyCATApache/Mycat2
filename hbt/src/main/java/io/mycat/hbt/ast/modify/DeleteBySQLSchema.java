package io.mycat.hbt.ast.modify;

import io.mycat.hbt.Op;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;

public class DeleteBySQLSchema extends Schema {
    private final String sql;

    public DeleteBySQLSchema(Op op, String sql) {
        super(op);
        this.sql = sql;
    }

    @Override
    public void accept(NodeVisitor visitor) {

    }
}
package io.mycat.hbt.ast.modify;

import io.mycat.hbt.Op;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.ast.query.FieldType;

import java.util.List;

public class ModifyTable extends Schema {
    private final String schema;
    private final String table;
    private String primaryColumn;

    public ModifyTable(String schema, String table, String primaryColumn) {
        super(Op.SCHEMA);
        this.schema = schema;
        this.table = table;
        this.primaryColumn = primaryColumn;
    }

    @Override
    public List<FieldType> fields() {
        return null;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
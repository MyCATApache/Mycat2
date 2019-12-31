package io.mycat.wu.ast.modify;

import io.mycat.wu.Op;
import io.mycat.wu.ast.base.NodeVisitor;
import io.mycat.wu.ast.base.Schema;
import io.mycat.wu.ast.query.FieldType;

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
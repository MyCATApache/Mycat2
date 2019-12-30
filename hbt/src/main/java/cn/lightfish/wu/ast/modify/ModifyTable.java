package cn.lightfish.wu.ast.modify;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.base.NodeVisitor;
import cn.lightfish.wu.ast.base.Schema;
import cn.lightfish.wu.ast.query.FieldType;

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
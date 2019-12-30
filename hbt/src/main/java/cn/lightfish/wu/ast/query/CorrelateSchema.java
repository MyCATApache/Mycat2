package cn.lightfish.wu.ast.query;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.base.Identifier;
import cn.lightfish.wu.ast.base.NodeVisitor;
import cn.lightfish.wu.ast.base.Schema;
import lombok.Data;

import java.util.List;

@Data
public class CorrelateSchema extends Schema {
    private final List<Identifier> columnName;
    private final Schema left;
    private final Schema right;
    private Op op;
    private Identifier refName;

    public CorrelateSchema(Op op, Identifier refName, List<Identifier> columnName, Schema left, Schema right) {
        super(op);
        this.op = op;
        this.columnName = columnName;
        this.left = left;
        this.right = right;
        this.refName = refName;
    }

    @Override
    public List<FieldType> fields() {
        return null;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String getAlias() {
        return null;
    }
}
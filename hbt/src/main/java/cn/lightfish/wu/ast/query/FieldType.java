package cn.lightfish.wu.ast.query;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.base.Node;
import cn.lightfish.wu.ast.base.NodeVisitor;
import lombok.Data;

@Data
public class FieldType extends Node {
    final String id;
    final String type;

    public FieldType(String id, String type) {
        super(Op.FIELD_SCHEMA);
        this.id = id;
        this.type = type;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}

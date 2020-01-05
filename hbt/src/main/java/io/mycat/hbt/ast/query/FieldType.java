package io.mycat.hbt.ast.query;

import io.mycat.hbt.Op;
import io.mycat.hbt.ast.base.Node;
import io.mycat.hbt.ast.base.NodeVisitor;
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

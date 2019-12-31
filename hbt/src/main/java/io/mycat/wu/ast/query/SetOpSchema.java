package io.mycat.wu.ast.query;

import io.mycat.wu.Op;
import io.mycat.wu.ast.base.NodeVisitor;
import io.mycat.wu.ast.base.Schema;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
public class SetOpSchema extends Schema {
    final List<Schema> schemas;

    public SetOpSchema(Op op, List<Schema> schemas) {
        super(op);
        this.schemas = new ArrayList<>(schemas);
    }

    @Override
    public List<FieldType> fields() {
        return Collections.unmodifiableList(schemas.get(0).fields());
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return "SetOpSchema(" +
                "op=" + op +
                ",list=" + schemas +
                ')';
    }
}
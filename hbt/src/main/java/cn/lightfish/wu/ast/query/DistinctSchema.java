package cn.lightfish.wu.ast.query;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.base.NodeVisitor;
import cn.lightfish.wu.ast.base.Schema;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class DistinctSchema extends Schema {
    private final Schema schema;

    public DistinctSchema(Schema schema) {
        super(Op.DISTINCT);
        this.schema = schema;
    }

    @Override
    public List<FieldType> fields() {
        return Collections.unmodifiableList(schema.fields());
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}

   


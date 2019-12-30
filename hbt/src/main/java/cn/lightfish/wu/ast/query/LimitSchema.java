package cn.lightfish.wu.ast.query;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.base.Literal;
import cn.lightfish.wu.ast.base.NodeVisitor;
import cn.lightfish.wu.ast.base.Schema;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class LimitSchema extends Schema {
    private final Schema schema;
    private final Literal offset;
    private final Literal limit;

    public LimitSchema(Schema schema, Literal offset, Literal limit) {
        super(Op.LIMIT);
        this.schema = schema;
        this.offset = offset;
        this.limit = limit;
    }

    public Literal getOffset() {
        return offset == null ? new Literal(0) : offset;
    }

    public Literal getLimit() {
        return limit;
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

package io.mycat.wu.ast.query;

import io.mycat.wu.Op;
import io.mycat.wu.ast.AggregateCall;
import io.mycat.wu.ast.base.GroupItem;
import io.mycat.wu.ast.base.NodeVisitor;
import io.mycat.wu.ast.base.Schema;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class GroupSchema extends Schema {
    private final Schema schema;
    private final List<GroupItem> keys;
    private final List<AggregateCall> exprs;

    public GroupSchema(Schema schema, List<GroupItem> keys, List<AggregateCall> exprs) {
        super(Op.GROUP);
        this.schema = schema;
        this.keys = keys;
        this.exprs = exprs;
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


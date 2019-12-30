package cn.lightfish.wu.ast.query;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.AggregateCall;
import cn.lightfish.wu.ast.base.GroupItem;
import cn.lightfish.wu.ast.base.NodeVisitor;
import cn.lightfish.wu.ast.base.Schema;
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


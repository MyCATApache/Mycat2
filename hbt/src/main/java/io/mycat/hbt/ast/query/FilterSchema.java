package io.mycat.hbt.ast.query;

import io.mycat.hbt.Op;
import io.mycat.hbt.ast.base.Expr;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Data;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Data
public class FilterSchema extends Schema {
    private final Schema schema;
    private final List<Expr> exprs;

    public FilterSchema(Schema schema, Expr... exprs) {
        super(Op.FILTER);
        this.schema = schema;
        this.exprs = Arrays.asList(exprs);
    }

    @Override
    public List<FieldType> fields() {
        return Collections.unmodifiableList(schema.fields());
    }

    public Schema getSchema() {
        return schema;
    }

    public List<Expr> getExpr() {
        return exprs;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}

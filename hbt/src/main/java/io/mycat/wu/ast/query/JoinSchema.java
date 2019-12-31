package io.mycat.wu.ast.query;

import io.mycat.wu.Op;
import io.mycat.wu.ast.base.Expr;
import io.mycat.wu.ast.base.NodeVisitor;
import io.mycat.wu.ast.base.Schema;
import lombok.Data;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Data
public class JoinSchema extends Schema {
    private final List<Schema> schemas;
    private final Expr condition;

    public JoinSchema(Op op, List<Schema> schemas, Expr condition) {
        super(op);
        this.schemas = schemas;
        this.condition = condition;
    }

    @Override
    public List<FieldType> fields() {
        ArrayList<FieldType> list = new ArrayList<>();
        for (Schema schema : schemas) {
            list.addAll(schema.fields());
        }
        return list;
    }

    public List<Schema> getSchemas() {
        return schemas;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return "JoinSchema(" +
                "type=" + op +
                ", schemas=" + schemas +
                ", condition=" + condition +
                ')';
    }
}
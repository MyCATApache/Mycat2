package io.mycat.wu.ast.query;

import io.mycat.wu.Op;
import io.mycat.wu.ast.base.Literal;
import io.mycat.wu.ast.base.NodeVisitor;
import io.mycat.wu.ast.base.Schema;
import lombok.Data;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Data
public class ValuesSchema extends Schema {
    private final List<Literal> values;
    private final List<FieldType> fieldNames;

    public ValuesSchema(List<FieldType> fieldNames, List<Literal> values) {
        super(Op.VALUES);
        this.fieldNames = fieldNames;
        this.values = values;
    }

    public ValuesSchema(List<FieldType> fieldNames, Literal... values) {
        this(fieldNames, Arrays.asList(values));
    }

    @Override
    public List<FieldType> fields() {
        return Collections.unmodifiableList(fieldNames);
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
package io.mycat.hbt.ast.query;

import io.mycat.hbt.HBTOp;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Getter;

import java.util.List;

@Getter
public class FromSqlSchema extends Schema {
    private final String sql;
    private final String targetName;
    private final List<FieldType> fieldTypes;

    public FromSqlSchema(List<FieldType> fieldTypes, String targetName, String sql) {
        super(HBTOp.FROM_SQL);
        this.fieldTypes = fieldTypes;
        this.targetName = targetName;
        this.sql = sql;
    }
    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}


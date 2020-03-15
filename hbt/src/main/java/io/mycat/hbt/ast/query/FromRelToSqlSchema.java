package io.mycat.hbt.ast.query;

import io.mycat.hbt.Op;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Getter;

@Getter
public class FromRelToSqlSchema extends Schema {
    String targetName;
    Schema rel;

    public FromRelToSqlSchema(String targetName,Schema rel) {
        super(Op.FROM_REL_TO_SQL);
        this.targetName = targetName;
        this.rel = rel;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
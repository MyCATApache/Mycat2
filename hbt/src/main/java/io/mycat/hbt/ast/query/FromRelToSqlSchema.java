package io.mycat.hbt.ast.query;

import io.mycat.hbt.ast.HBTOp;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class FromRelToSqlSchema extends Schema {
    String targetName;
    Schema rel;

    public FromRelToSqlSchema(String targetName,Schema rel) {
        super(HBTOp.FROM_REL_TO_SQL);
        this.targetName = targetName;
        this.rel = rel;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
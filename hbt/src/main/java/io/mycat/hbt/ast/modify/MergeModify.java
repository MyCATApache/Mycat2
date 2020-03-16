package io.mycat.hbt.ast.modify;

import io.mycat.hbt.ast.HBTOp;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class MergeModify extends Schema {
    private final Iterable<ModifyFromSql> list;

    public MergeModify(Iterable<ModifyFromSql> list) {
        super(HBTOp.MERGE_MODIFY);
        this.list = list;
    }


    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
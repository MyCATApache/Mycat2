package io.mycat.hbt.ast.query;

import io.mycat.hbt.HBTOp;
import io.mycat.hbt.ast.base.Expr;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Getter;

import java.util.List;

@Getter
public class FilterFromTableSchema extends Schema {
    final Expr filter;
    final List<String> names;

    public FilterFromTableSchema( Expr filter, List<String> names) {
        super(HBTOp.FILTER_FROM_TABLE);
        this.filter = filter;
        this.names = names;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
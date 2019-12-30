package cn.lightfish.wu.ast.query;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.base.Identifier;
import cn.lightfish.wu.ast.base.NodeVisitor;
import cn.lightfish.wu.ast.base.Schema;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class FromSchema extends Schema {
    private List<Identifier> names;

    public FromSchema(List<Identifier> names) {
        super(Op.FROM);
        this.names = names;
    }

    @Override
    public List<FieldType> fields() {
        return Collections.emptyList();
    }

    @Override
    public String getAlias() {
        return names.get(names.size() - 1).getValue();
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}


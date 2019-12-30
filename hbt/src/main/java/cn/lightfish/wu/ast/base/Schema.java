package cn.lightfish.wu.ast.base;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.query.FieldType;
import lombok.Data;

import java.util.List;

@Data
public abstract class Schema extends Node {
    public Schema(Op op) {
        super(op);
    }

    public abstract List<FieldType> fields();

    public String getAlias() {
        return null;
    }
}

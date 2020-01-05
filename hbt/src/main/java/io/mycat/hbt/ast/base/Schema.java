package io.mycat.hbt.ast.base;

import io.mycat.hbt.Op;
import io.mycat.hbt.ast.query.FieldType;
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

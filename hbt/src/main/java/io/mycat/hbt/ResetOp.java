package io.mycat.hbt;

import io.mycat.hbt.ast.base.Identifier;
import io.mycat.hbt.ast.base.Node;
import io.mycat.hbt.ast.other.ResetStatement;

import static io.mycat.hbt.LevelType.SESSION;

public interface ResetOp {

    static ResetStatement reset(LevelType leve1, String identifier) {
        return new ResetStatement(leve1, identifier);
    }

    static ResetStatement reset(String identifier) {
        return reset(SESSION, identifier);
    }

    static ResetStatement reset(LevelType leve1, Node identifier) {
        return reset(leve1, identifier);
    }

    static ResetStatement reset(Node identifier) {
        Identifier id = (Identifier) identifier;
        return reset(id.getValue());
    }
}
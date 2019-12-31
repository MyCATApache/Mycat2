package io.mycat.wu;

import io.mycat.wu.ast.base.Identifier;
import io.mycat.wu.ast.base.Node;
import io.mycat.wu.ast.other.ResetStatement;

import static io.mycat.wu.LevelType.SESSION;

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
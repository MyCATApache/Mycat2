package cn.lightfish.wu;

import cn.lightfish.wu.ast.base.Identifier;
import cn.lightfish.wu.ast.base.Node;
import cn.lightfish.wu.ast.other.ResetStatement;

import static cn.lightfish.wu.LevelType.SESSION;

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
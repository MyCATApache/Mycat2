package cn.lightfish.wu.ast.other;

import cn.lightfish.wu.LevelType;

public class ResetStatement {
    private final LevelType level;
    private final String identifier;

    public ResetStatement(LevelType level, String identifier) {
        this.level = level;
        this.identifier = identifier;
    }
}
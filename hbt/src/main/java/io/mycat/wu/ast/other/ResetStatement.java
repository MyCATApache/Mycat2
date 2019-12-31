package io.mycat.wu.ast.other;

import io.mycat.wu.LevelType;

public class ResetStatement {
    private final LevelType level;
    private final String identifier;

    public ResetStatement(LevelType level, String identifier) {
        this.level = level;
        this.identifier = identifier;
    }
}
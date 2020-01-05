package io.mycat.hbt.ast.other;

import io.mycat.hbt.LevelType;

public class ResetStatement {
    private final LevelType level;
    private final String identifier;

    public ResetStatement(LevelType level, String identifier) {
        this.level = level;
        this.identifier = identifier;
    }
}
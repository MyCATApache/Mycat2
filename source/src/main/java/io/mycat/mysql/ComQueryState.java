package io.mycat.mysql;

public enum ComQueryState {
    QUERY_PACKET(true),
    FIRST_PACKET(true),
    COLUMN_DEFINITION(false),
    COLUMN_END_EOF(true),
    RESULTSET_ROW(false),
    RESULTSET_ROW_END(true),
    PREPARE_FIELD(false),
    PREPARE_FIELD_EOF(true),
    PREPARE_PARAM(false),
    PREPARE_PARAM_EOF(true),
    END(false);
    boolean needFull;

    ComQueryState(boolean needFull) {
        this.needFull = needFull;
    }
}
package io.mycat.mysql;

public enum ComQueryState {
    DO_NOT(false),
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
    RESP_END(false),
    LOCAL_INFILE_REQUEST(true),
    LOCAL_INFILE_FILE_CONTENT(true),
    LOCAL_INFILE_EMPTY_PACKET(true),
    LOCAL_INFILE_OK_PACKET(true);
    boolean needFull;

    ComQueryState(boolean needFull) {
        this.needFull = needFull;
    }
}
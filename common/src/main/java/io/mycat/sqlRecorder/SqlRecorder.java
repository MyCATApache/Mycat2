package io.mycat.sqlRecorder;

/**
 * PARSE_SQL,
 * COMPILE_SQL,//compile
 * RBO,
 * CBO,
 * GET_CONNECTION,
 * CONNECTION_QUERY_RESPONSE,
 * RESPONSE
 */
public interface SqlRecorder {
    public SqlRecord startRecord(SqlRecorderType type, String host, String userName, String sql);

    public SqlRecord addRecord(SqlRecorderType type, String host, long value);

    public SqlRecord endRecord(String host);
}
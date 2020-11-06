package io.mycat.sqlrecorder;

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
    public void start();
    public void addRecord(SqlRecorderType type, String sql, long value);
}
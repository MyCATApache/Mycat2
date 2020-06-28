package io.mycat.sqlRecorder;

import com.google.common.cache.CacheBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public enum SqlRecorderRuntime implements SimpleAnalyzer {
    INSTANCE;
    static final CopyOnWriteArrayList<RecordContext> all = new CopyOnWriteArrayList<>();
    final static int CONST = 8192;
    static final ThreadLocal<RecordContext> cacheThreadLocal = ThreadLocal.withInitial(() -> {
        RecordContext recordContext = new RecordContext();
        recordContext.index = 0;
        recordContext.map = (ConcurrentMap) CacheBuilder.newBuilder().maximumSize(CONST).build().asMap();
        all.add(recordContext);
        return recordContext;
    });


    /**
     * 丢弃首次请求
     *
     * @return
     */
    public SqlRecorder getCurrentRecorder() {
        return cacheThreadLocal.get();
    }

    private static SqlRecord[] createArray(String s) {
        SqlRecord[] sqlRecords1 = new SqlRecord[CONST];
        for (int i = 0; i < CONST; i++) {
            sqlRecords1[i] = new SqlRecord();
        }
        return sqlRecords1;
    }

    public void reset() {
        for (RecordContext recordContext : all) {
            recordContext.map.clear();
        }

    }

    static class RecordContext implements SqlRecorder {
        int index;
        ConcurrentMap<String, SqlRecord[]> map;

        @Override
        public void start() {
            if (this.index >= CONST || this.index < 0) {
                this.index = 0;
            } else {
                this.index++;
            }
        }

        @Override
        public void addRecord(SqlRecorderType type, String sql, long value) {
            SqlRecord[] sqlRecords = this.map.computeIfAbsent(sql, SqlRecorderRuntime::createArray);
            SqlRecord c = sqlRecords[this.index];
            c.statement = sql;
            switch (type) {
                case AT_START:
                    c.startTime = value;
                    break;
                case PARSE_SQL:
                    c.parseTime = value;
                    break;
                case COMPILE_SQL:
                    c.compileTime = value;
                    break;
                case RBO:
                    c.rboTime = value;
                    break;
                case CBO:
                    c.cboTime = value;
                    break;
                case GET_CONNECTION:
                    c.connectionPoolTime = Math.max(c.connectionPoolTime, value);
                    break;
                case CONNECTION_QUERY_RESPONSE:
                    c.connectionQueryTime = Math.max(c.connectionQueryTime, value);
                    break;
                case EXECUTION_TIME:
                    c.executionTime = value;
                    break;
                case AT_END:
                    c.endTime = value;
                    break;
            }
        }
    }


    @Override
    public Map<String, SqlRecord> getRecords() {
        HashMap<String, SqlRecord> map = new HashMap<>();
        for (RecordContext recordContext : all) {
            for (Map.Entry<String, SqlRecord[]> stringEntry : recordContext.map.entrySet()) {
                String statement = stringEntry.getKey();

                double sqlRows = 0;
                double netInBytes = 0;
                double netOutBytes = 0;
                double parseTime = 0;
                double compileTime = 0;
                double cboTime = 0;
                double rboTime = 0;
                double connectionPoolTime = 0;
                double connectionQueryTime = 0;
                double wholeTime = 0;
                double execution_time = 0;
                SqlRecord[] value = stringEntry.getValue();

                int count = 0;

                //总是不统计首位的记录
                for (int i = 0; i < value.length - 1; i++) {
                    SqlRecord record = value[i];
                    if (record == null || record.getStatement() == null) {
                        continue;
                    }
                    count++;
                    wholeTime += record.getEndTime() - record.getStartTime();
                    sqlRows += record.getSqlRows();
                    netInBytes += record.getNetInBytes();
                    netOutBytes += record.getNetOutBytes();
                    parseTime += record.getParseTime();
                    compileTime += record.getCompileTime();
                    cboTime += record.getCboTime();
                    rboTime += record.getRboTime();
                    connectionPoolTime += record.getConnectionPoolTime();
                    connectionQueryTime += record.getConnectionQueryTime();
                    execution_time+=record.getExecutionTime();
                }
                if (count > 0) {
                    SqlRecord record = new SqlRecord();
                    record.statement = statement;
                    record.connectionQueryTime = (long) connectionQueryTime / count;
                    record.connectionPoolTime = (long) connectionPoolTime / count;
                    record.cboTime = (long) cboTime / count;
                    record.rboTime = (long) rboTime / count;
                    record.parseTime = (long) parseTime / count;
                    record.netInBytes = (long) netInBytes / count;
                    record.netOutBytes = (long) netOutBytes / count;
                    record.compileTime = (long) compileTime / count;
                    record.wholeTime = (long) wholeTime / count;
                    record.sqlRows = (long) sqlRows / count;
                    record.executionTime= (long) execution_time / count;
                    map.put(statement, record);
                }
            }
        }
        return map;
    }
}
package io.mycat.sqlRecorder;

import com.google.common.cache.CacheBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public enum SqlRecorderRuntime implements SqlRecorder, SimpleAnalyzer {
    INSTANCE;
    final CopyOnWriteArrayList<RecordContext> all = new CopyOnWriteArrayList<>();
    final ThreadLocal<RecordContext> cacheThreadLocal = ThreadLocal.withInitial(() -> {
        RecordContext recordContext = new RecordContext();
        recordContext.index = 0;
        recordContext.map = (ConcurrentMap) CacheBuilder.newBuilder().maximumSize(CONST).build().asMap();
        all.add(recordContext);
        return recordContext;
    });
    final static int CONST = 8192;


    private static SqlRecord[] createArray(String s) {
        SqlRecord[] sqlRecords1 = new SqlRecord[CONST];
        for (int i = 0; i < CONST; i++) {
            sqlRecords1[i] = new SqlRecord();
        }
        return sqlRecords1;
    }

    @Override
    public void start() {
        RecordContext recordContext = cacheThreadLocal.get();
        recordContext.index++;
        if (recordContext.index >= CONST) {
            recordContext.index = 0;
        }
    }

    static class RecordContext {
        int index;
        ConcurrentMap<String, SqlRecord[]> map;
    }

    @Override
    public void addRecord(SqlRecorderType type, String sql, long value) {
        RecordContext recordContext = cacheThreadLocal.get();
        SqlRecord[] sqlRecords = recordContext.map.computeIfAbsent(sql, SqlRecorderRuntime::createArray);
        SqlRecord c = sqlRecords[recordContext.index];
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
                c.connectionPoolTime = value;
                break;
            case CONNECTION_QUERY_RESPONSE:
                c.connectionQueryTIme = value;
                break;
            case AT_END:
                c.endTime = value;
                break;
        }
    }

    @Override
    public Map<String, SqlRecord> getRecords() {
        HashMap<String,SqlRecord> map = new HashMap<>();
        for (RecordContext recordContext : all) {
            for (Map.Entry<String, SqlRecord[]> stringEntry : recordContext.map.entrySet()) {
                String statement = stringEntry.getKey();
                long   sqlRows =0;
                long  netInBytes = 0;
                long  netOutBytes = 0;
                long   parseTime =0;
                long    compileTime = 0;
                long   cboTime =0;
                long   rboTime = 0;
                long   connectionPoolTime = 0;
                long   connectionQueryTIme = 0;
                long wholeTime = 0;
                SqlRecord[] value = stringEntry.getValue();
                for (SqlRecord record : value) {
                    if (record == null || record.getStatement() == null) {
                        continue;
                    }
                    wholeTime += record.getEndTime() -  record.getStartTime();
                    sqlRows += record.getSqlRows();
                    netInBytes += record.getNetInBytes();
                    netOutBytes += record.getNetOutBytes();
                    parseTime += record.getParseTime();
                    compileTime += record.getCompileTime();
                    cboTime += record.getCboTime();
                    rboTime += record.getRboTime();
                    connectionPoolTime += record.getConnectionPoolTime();
                    connectionQueryTIme += record.getConnectionQueryTIme();
                }
                SqlRecord record = new SqlRecord();
                record.connectionQueryTIme = connectionQueryTIme;
                record.connectionPoolTime = connectionPoolTime;
                record.cboTime = cboTime;
                record.rboTime = rboTime;
                record.parseTime = parseTime;
                record.netInBytes = netInBytes;
                record.netOutBytes = netOutBytes;
                record.compileTime = compileTime;
                record.wholeTime = wholeTime;
                record.sqlRows =sqlRows;
                map.put(statement,record);
            }
        }
        return map;
    }
}
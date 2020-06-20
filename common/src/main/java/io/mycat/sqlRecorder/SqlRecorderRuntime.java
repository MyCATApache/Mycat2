package io.mycat.sqlRecorder;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

public enum SqlRecorderRuntime implements SqlRecorder, SimpleAnalyzer {
    INSTANCE;
    final Cache<String, SqlRecord> cache = CacheBuilder.newBuilder().maximumSize(1024 * 5).build();

    @Override
    public SqlRecord startRecord(SqlRecorderType type, String host, String userName, String sql) {
        ConcurrentMap<String, SqlRecord> map = cache.asMap();
        SqlRecord sqlRecord = map.computeIfAbsent(host, s -> SqlRecord.builder().host(host).username(userName)
                .statement(sql).build());
        return sqlRecord;
    }

    @Override
    public SqlRecord addRecord(SqlRecorderType type, String host, long value) {
        SqlRecord record;
        Optional.ofNullable(record = cache.getIfPresent(host)).ifPresent(c -> {
            switch (type) {
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
                case RESPONSE:
                    c.endTime = value;
                    break;
            }
        });
        return record;
    }

    @Override
    public SqlRecord endRecord(String host) {
        SqlRecord record;
        Optional.ofNullable(record = cache.getIfPresent(host)).ifPresent(c -> {
            c.endTime = System.currentTimeMillis();
        });
        return record;
    }

    @Override
    public Map<String, SqlRecord> getRecords() {
        return cache.asMap();
    }
}
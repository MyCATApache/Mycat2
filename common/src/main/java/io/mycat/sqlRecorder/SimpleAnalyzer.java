package io.mycat.sqlRecorder;

import com.google.common.cache.Cache;

import java.util.List;
import java.util.Map;

public interface SimpleAnalyzer {
    Map<String, SqlRecord> getRecords();
    Map<String, List<SqlRecord>> getRecordList();
}
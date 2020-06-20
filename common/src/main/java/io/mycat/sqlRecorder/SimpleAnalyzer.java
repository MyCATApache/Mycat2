package io.mycat.sqlRecorder;

import com.google.common.cache.Cache;

import java.util.Map;

public interface SimpleAnalyzer {
    Map<String, SqlRecord> getRecords();
}
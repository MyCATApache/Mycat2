package io.mycat.sqlrecorder;

import java.util.List;
import java.util.Map;

public interface SimpleAnalyzer {
    Map<String, SqlRecord> getRecords();
    Map<String, List<SqlRecord>> getRecordList();
}
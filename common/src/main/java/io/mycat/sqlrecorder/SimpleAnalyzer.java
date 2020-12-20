package io.mycat.sqlrecorder;

import java.util.List;
import java.util.Map;

public interface SimpleAnalyzer {
    List<SqlRecord> getRecords();

    void addSqlRecord(SqlRecord record);
}
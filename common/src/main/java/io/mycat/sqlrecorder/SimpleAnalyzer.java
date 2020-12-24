package io.mycat.sqlrecorder;

import java.util.List;

public interface SimpleAnalyzer {
    List<SqlRecord> getRecords();

    void addSqlRecord(SqlRecord record);
}
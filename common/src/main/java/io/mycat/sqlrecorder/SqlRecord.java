package io.mycat.sqlrecorder;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class SqlRecord implements Comparable<SqlRecord> {
    double executionTime;
    String statement;
    double startTime;
    double endTime;
    double sqlRows;
    double netInBytes;
    double netOutBytes;
    double parseTime;
    double compileTime;
    double cboTime;
    double rboTime;
    double connectionPoolTime;
    double connectionQueryTime;
    double wholeTime;

    @Override
    public int compareTo(@NotNull SqlRecord o) {
        return this.statement.compareTo(o.statement);
    }
}
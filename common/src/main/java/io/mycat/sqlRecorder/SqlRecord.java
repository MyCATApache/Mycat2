package io.mycat.sqlRecorder;

import lombok.Builder;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class SqlRecord implements Comparable<SqlRecord>{
    String statement;
    long startTime;
    long endTime;
    long sqlRows;
    long netInBytes;
    long netOutBytes;
    long parseTime;
    long compileTime;
    long cboTime;
    long rboTime;
    long connectionPoolTime;
    long connectionQueryTime;
    long wholeTime;

    @Override
    public int compareTo(@NotNull SqlRecord o) {
        return this.statement.compareTo(o.statement);
    }
}
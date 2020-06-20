package io.mycat.sqlRecorder;

import lombok.Builder;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
@Builder
public class SqlRecord implements Comparable<SqlRecord>{
    String host;
    String statement;
    String username;
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
    long connectionQueryTIme;

    @Override
    public int compareTo(@NotNull SqlRecord o) {
        return this.host.compareTo(o.host);
    }
}
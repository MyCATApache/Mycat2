package io.mycat.beans.log.monitor;

public interface MycatSQLLogMonitor {
    public void pushSqlLog(SqlEntry sqlEntry);
}

package io.mycat.monitor;

import com.alibaba.druid.sql.parser.SQLType;
import com.imadcn.framework.idworker.algorithm.Snowflake;
import io.mycat.MycatDataContext;

public abstract class MycatSQLLogMonitor {
    private final long workerId;
    private final Snowflake snowflake;

    public MycatSQLLogMonitor(long workerId) {
        this.workerId = workerId;
        this.snowflake = Snowflake.create(workerId);
    }

    public LogEntryHolder startRecord(MycatDataContext context,
                                      String hash,
                                      SQLType sqlType,
                                      String sql) {
        return new LogEntryHolder(context, hash, sqlType, sql, String.valueOf(this.snowflake.nextId()), this);
    }


    abstract protected void pushSqlLog(SqlEntry sqlEntry);

    public abstract void setSqlTimeFilter(long value);

    public  abstract long getSqlTimeFilter();
}

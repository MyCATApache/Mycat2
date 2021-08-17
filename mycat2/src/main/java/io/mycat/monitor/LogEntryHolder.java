package io.mycat.monitor;

import com.alibaba.druid.sql.parser.SQLType;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.config.ServerConfig;
import io.mycat.exporter.SqlRecorderRuntime;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;

public class LogEntryHolder {

    SqlEntry sqlEntry;
    long start;

    final MycatDataContext mycatDataContext;
    final MycatSQLLogMonitor mycatSQLLogMonitor;

    public LogEntryHolder(MycatDataContext context,
                          String hash,
                          SQLType sqlType,
                          String sql,
                          String traceId,
                          MycatSQLLogMonitor mycatSQLLogMonitor) {
        this.mycatDataContext = context;
        this.mycatSQLLogMonitor = mycatSQLLogMonitor;

        String connectionId = String.valueOf(mycatDataContext.getSessionId());
        String user = mycatDataContext.getUser().getUserName();
        String transactionId = mycatDataContext.getTransactionSession().getXid();
        String ip = mycatDataContext.getUser().getRemoteAddress().getHostString();
        int port = mycatDataContext.getUser().getRemoteAddress().getPort();
        ServerConfig serverConfig
                = MetaClusterCurrent.wrapper(ServerConfig.class);
        int instanceId = (serverConfig.getMycatId());
        this.sqlEntry = SqlEntry.create(instanceId, user, connectionId, ip, port, traceId, hash, sqlType, sql, transactionId);
        this.start = System.currentTimeMillis();
    }


    public void recordSQLEnd(boolean result,
                             Map<String, Object> targets,
                             String externalMessage) {
        try {
            Objects.requireNonNull(this.sqlEntry);
            long now = System.currentTimeMillis();
            long time = now - start;
            this.sqlEntry.end(time, LocalDateTime.now(), mycatDataContext.getAffectedRows(), result, targets, externalMessage);
            mycatSQLLogMonitor.pushSqlLog(this.sqlEntry);
            InstanceMonitor.plusLrt(time);

            SqlRecorderRuntime recorderRuntime = MetaClusterCurrent.wrapper(SqlRecorderRuntime.class);
            recorderRuntime.addSqlRecord(this.sqlEntry);
        } finally {
            this.sqlEntry = null;
        }

    }
}

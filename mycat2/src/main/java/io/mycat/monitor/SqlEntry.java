package io.mycat.monitor;

import com.alibaba.druid.sql.parser.SQLType;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.util.Map;

@Data
public class SqlEntry implements LogEntry ,Comparable<SqlEntry>{
    int instanceId;
    String user;
    String connectionId;
    String ip;
    int port;
    String traceId;
    String hash;
    SQLType sqlType;
    String sql;
    String transactionId;

    long sqlTime;
    LocalDateTime responseTime;
    long affectRow;
    boolean result;
    String externalMessage;


    public  static SqlEntry create(
            int instanceId,
            String user,
            String connectionId,
            String ip,
            int port,
            String traceId,
            String hash,
            SQLType sqlType,
            String sql,
            String transactionId
    ) {
        SqlEntry sqlEntry = new SqlEntry();
        sqlEntry.setInstanceId(instanceId);
        sqlEntry.setUser(user);
        sqlEntry.setConnectionId(connectionId);
        sqlEntry.setIp(ip);
        sqlEntry.setPort(port);
        sqlEntry.setTraceId(traceId);
        sqlEntry.setHash(hash);
        sqlEntry.setSqlType(sqlType);
        sqlEntry.setSql(sql);
        sqlEntry.setTransactionId(transactionId);
        return sqlEntry;
    }

    public  SqlEntry end(
            long sqlTime,
            LocalDateTime responseTime,
            long affectRow,
            boolean result,
            Map<String, Object> targets,
            String externalMessage) {
        SqlEntry sqlEntry = this;
        sqlEntry.setSqlTime(sqlTime);
        sqlEntry.setResponseTime(responseTime);
        sqlEntry.setAffectRow(affectRow);
        sqlEntry.setResult(result);
        sqlEntry.setExternalMessage(externalMessage);
        return sqlEntry;
    }

    @Override
    public int compareTo(@NotNull SqlEntry o) {
        return (int)(this.sqlTime - o.getSqlTime());
    }
}

package io.mycat.exporter;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.IOExecutor;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.monitor.SqlEntry;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

public class MySQLLogConsumer implements Consumer<SqlEntry> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLLogConsumer.class);
    boolean init = false;
    @SneakyThrows
    public MySQLLogConsumer() {


    }

    private void init() throws SQLException {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(metadataManager.getPrototype())) {
            Connection rawConnection = connection.getRawConnection();
            rawConnection.setSchema("mycat");
            JdbcUtils.execute(connection.getRawConnection(),
                    " create table if not exists  `sql_log` (\n" +
                            "  `instanceId` bigint(20) DEFAULT NULL,\n" +
                            "  `user` varchar(64) DEFAULT NULL,\n" +
                            "  `connectionId` bigint(20) DEFAULT NULL,\n" +
                            "  `ip` varchar(22) DEFAULT NULL,\n" +
                            "  `port` bigint(20) DEFAULT NULL,\n" +
                            "  `traceId` varchar(22) NOT NULL,\n" +
                            "  `hash` varchar(22) DEFAULT NULL,\n" +
                            "  `sqlType` varchar(22) DEFAULT NULL,\n" +
                            "  `sql` tinytext,\n" +
                            "  `transactionId` varchar(22) DEFAULT NULL,\n" +
                            "  `sqlTime` time DEFAULT NULL,\n" +
                            "  `responseTime` datetime DEFAULT NULL,\n" +
                            "  `affectRow` int(11) DEFAULT NULL,\n" +
                            "  `result` tinyint(1) DEFAULT NULL,\n" +
                            "  `externalMessage` tinytext,\n" +
                            "  PRIMARY KEY (`traceId`)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", Collections.emptyList());
        }
    }

    @Override
    @SneakyThrows
    public void accept(SqlEntry sqlEntry) {
        if (!init){
            init = true;
            try {
                init();
            }catch (Exception e){
                LOGGER.error("",e);
            }
        }
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        ioExecutor.executeBlocking(new Handler<Promise<Void>>() {
            @Override

            public void handle(Promise<Void> event) {
                try {
                    try (DefaultConnection connection = jdbcConnectionManager.getConnection(metadataManager.getPrototype())) {
                        JdbcUtils.execute(connection.getRawConnection(), "INSERT INTO `mycat`.`sql_log` (" +
                                        "`instanceId`," +
                                        "user," +
                                        "connectionId," +
                                        "ip," +
                                        "port," +
                                        "traceId," +
                                        "hash," +
                                        "sqlType," +
                                        "`sql`," +
                                        "transactionId," +
                                        "sqlTime," +
                                        "responseTime," +
                                        "affectRow," +
                                        "result," +
                                        "externalMessage)" +
                                        "values(?,?,?,?,?," +
                                        "?,?,?,?,?," +
                                        "?,?,?,?,?)",
                                Arrays.asList(sqlEntry.getInstanceId(),
                                        sqlEntry.getUser(),
                                        sqlEntry.getConnectionId(),
                                        sqlEntry.getIp(),
                                        sqlEntry.getPort(),
                                        sqlEntry.getTraceId(),
                                        sqlEntry.getHash(),
                                        Objects.toString(sqlEntry.getSqlType()),
                                        sqlEntry.getSql(),
                                        sqlEntry.getTransactionId(),
                                        sqlEntry.getSqlTime(),
                                        sqlEntry.getResponseTime(),
                                        sqlEntry.getAffectRow(),
                                        sqlEntry.isResult(),
                                        sqlEntry.getExternalMessage()
                                ));
                    }
                } catch (Exception e) {
                    LOGGER.error("", e);
                }finally {
                    event.tryComplete();
                }
            }
        });

    }
}

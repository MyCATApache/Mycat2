package io.mycat.ddl.executer;

import io.mycat.MetaClusterCurrent;
import io.mycat.MycatWorkerProcessor;
import io.mycat.config.ShardingTableConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;

import java.util.Set;

public class DDLExecuter {

    public static void createNormalTable(String createTableSql) {
        createNormalTable(createTableSql, "prototype");
    }

    public static void createNormalTable(String createTableSql, String targetName) {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
            connection.executeUpdate(createTableSql, false);
        }
    }

    public static void createGlobalTable(String createTableSql) {
        createNormalTable(createTableSql);
        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
        mycatWorkerProcessor.getMycatWorker().execute(()->{
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            Set<String> targetNames = jdbcConnectionManager.getDatasourceInfo().keySet();
            for (String targetName : targetNames) {
                createNormalTable(createTableSql, targetName);
            }
        });
    }

    public static void createShardingTable(ShardingTableConfig shardingTableConfig) {
        createNormalTable(shardingTableConfig.getCreateTableSQL());
    }
}

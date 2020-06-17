package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class ShowDatasourceCommand implements MycatCommand {
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        if ("show @@backend.datasource".equalsIgnoreCase(request.getText())) {
            Optional<JdbcConnectionManager> connectionManager = Optional.ofNullable(JdbcRuntime.INSTANCE.getConnectionManager());
            Collection<JdbcDataSource> jdbcDataSources = connectionManager.map(i -> i.getDatasourceInfo()).map(i -> i.values()).orElse(Collections.emptyList());
            ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();

            resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("IP", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("PORT", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("USERNAME", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("PASSWORD", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("MAX_CON", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("MIN_CON", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("USED_CON", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("MAX_RETRY_COUNT", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("MAX_CONNECT_TIMEOUT", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("DB_TYPE", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("URL", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("WEIGHT", JDBCType.VARCHAR);

            resultSetBuilder.addColumnInfo("INIT_SQL", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("INIT_SQL_GET_CONNECTION", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("INSTANCE_TYPE", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("IDLE_TIMEOUT", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("DRIVER", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("IS_MYSQL", JDBCType.BOOLEAN);

            for (JdbcDataSource jdbcDataSource : jdbcDataSources) {
                DatasourceRootConfig.DatasourceConfig config = jdbcDataSource.getConfig();
                String NAME = config.getName();
                String IP = config.getIp();
                int PORT = config.getPort();
                String USERNAME = config.getUser();
                String PASSWORD = config.getPassword();
                int MAX_CON = config.getMaxCon();
                int MIN_CON = config.getMinCon();
                int USED_CON = jdbcDataSource.getUsedCount();
                int MAX_RETRY_COUNT = config.getMaxRetryCount();
                long MAX_CONNECT_TIMEOUT = config.getMaxConnectTimeout();
                String DB_TYPE = config.getDbType();
                String URL = config.getUrl();
                int WEIGHT = config.getWeight();
                String INIT_SQL = Optional.ofNullable(config.getInitSqls()).map(o -> String.join(";", o)).orElse("");
                boolean INIT_SQL_GET_CONNECTION = config.isInitSqlsGetConnection();

                String INSTANCE_TYPE = Optional.ofNullable(ReplicaSelectorRuntime.INSTANCE.getPhysicsInstanceByName(NAME)).map(i -> i.getType().name()).orElse(config.getInstanceType());
                long IDLE_TIMEOUT = config.getIdleTimeout();
                String DRIVER = jdbcDataSource.getDataSource().toString();//保留属性
                String TYPE = config.getType();
                boolean IS_MYSQL = jdbcDataSource.isMySQLType();

                resultSetBuilder.addObjectRowPayload(Arrays.asList(NAME, IP, PORT, USERNAME, PASSWORD, MAX_CON, MIN_CON, USED_CON,
                        MAX_RETRY_COUNT, MAX_CONNECT_TIMEOUT, DB_TYPE, URL, WEIGHT, INIT_SQL, INIT_SQL_GET_CONNECTION, INSTANCE_TYPE,
                        IDLE_TIMEOUT, DRIVER, TYPE, IS_MYSQL));
            }
            response.sendResultSet(() -> resultSetBuilder.build());
            return true;
        }
        return false;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        return false;
    }

    @Override
    public String getName() {
        return getClass().getName();
    }
}
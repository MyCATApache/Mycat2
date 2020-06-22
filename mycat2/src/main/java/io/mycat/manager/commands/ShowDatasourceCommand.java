package io.mycat.manager.commands;

import io.mycat.MycatConfig;
import io.mycat.MycatCore;
import io.mycat.MycatDataContext;
import io.mycat.RootHelper;
import io.mycat.beans.MySQLDatasource;
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
import java.util.*;
import java.util.stream.Collectors;

public class ShowDatasourceCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@backend.datasource";
    }

    @Override
    public String description() {
        return "show the datasource info";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        MycatConfig mycatConfig = RootHelper.INSTANCE.getConfigProvider().currentConfig();
        Map<String, DatasourceRootConfig.DatasourceConfig> datasourceConfigMap = mycatConfig.getDatasource().getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
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
        resultSetBuilder.addColumnInfo("EXIST_CON", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("USE_CON", JDBCType.BIGINT);
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

            int USED_CON = jdbcDataSource.getUsedCount();//注意显示顺序
            int EXIST_CON = USED_CON;//jdbc连接池已经存在连接数量是内部状态,未知
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

            resultSetBuilder.addObjectRowPayload(Arrays.asList(NAME, IP, PORT, USERNAME, PASSWORD, MAX_CON, MIN_CON, EXIST_CON,USED_CON,
                    MAX_RETRY_COUNT, MAX_CONNECT_TIMEOUT, DB_TYPE, URL, WEIGHT, INIT_SQL, INIT_SQL_GET_CONNECTION, INSTANCE_TYPE,
                    IDLE_TIMEOUT, DRIVER, TYPE, IS_MYSQL));
        }

        for (MySQLDatasource value : MycatCore.INSTANCE.getDatasourceMap().values()) {
            String NAME = value.getName();
            Optional<DatasourceRootConfig.DatasourceConfig> e = Optional.ofNullable(datasourceConfigMap.get(NAME));

            String IP = value.getIp();
            int PORT = value.getPort();
            String USERNAME = value.getUsername();
            String PASSWORD = value.getPassword();
            int MAX_CON = value.getSessionLimitCount();
            int MIN_CON = value.getSessionMinCount();
            int USED_CON = value.getUsedCounter();
            int EXIST_CON = value.getConnectionCounter();
            int MAX_RETRY_COUNT = value.gerMaxRetry();
            long MAX_CONNECT_TIMEOUT = value.getMaxConnectTimeout();
            String DB_TYPE = "mysql";
            String URL = null;
            int WEIGHT = e.map(i->i.getWeight()).orElse(-1);
            String INIT_SQL = value.getInitSqlForProxy();
            boolean INIT_SQL_GET_CONNECTION = false;

            String INSTANCE_TYPE = Optional.ofNullable(ReplicaSelectorRuntime.INSTANCE.getPhysicsInstanceByName(NAME)).map(i -> i.getType().name()).orElse(e.map(i->i.getInstanceType()).orElse(null));
            long IDLE_TIMEOUT = value.getIdleTimeout();

            String DRIVER = "native";//保留属性
            String TYPE =  e.map(i->i.getType()).orElse(null);
            boolean IS_MYSQL = true;

            resultSetBuilder.addObjectRowPayload(Arrays.asList(NAME, IP, PORT, USERNAME, PASSWORD, MAX_CON, MIN_CON,EXIST_CON, USED_CON,
                    MAX_RETRY_COUNT, MAX_CONNECT_TIMEOUT, DB_TYPE, URL, WEIGHT, INIT_SQL, INIT_SQL_GET_CONNECTION, INSTANCE_TYPE,
                    IDLE_TIMEOUT, DRIVER, TYPE, IS_MYSQL));
        }


        response.sendResultSet(() -> resultSetBuilder.build());
    }

}
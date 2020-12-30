package io.mycat.assemble;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


public interface MycatTest {

    String DB_MYCAT = System.getProperty("db_mycat","jdbc:mysql://127.0.0.1:8066/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
    String DB1 = System.getProperty("db1","jdbc:mysql://127.0.0.1:3306/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
    String DB2 = System.getProperty("db2","jdbc:mysql://127.0.0.1:3307/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");

    String RESET_CONFIG ="/*+ mycat:resetConfig{} */";

    Map<String, DruidDataSource> dsMap = new ConcurrentHashMap<>();
    Logger LOGGER = LoggerFactory.getLogger(MycatTest.class);


    default Connection getMySQLConnection(String url) throws Exception {
        return dsMap.computeIfAbsent(url, new Function<String, DruidDataSource>() {
            @Override
            @SneakyThrows
            public DruidDataSource apply(String url) {
                Map<String, String> urlParameters = JsonUtil.urlSplit(url);
                String username = urlParameters.getOrDefault("username","root");
                String password = urlParameters.getOrDefault("password","123456");

                DruidDataSource dataSource = new DruidDataSource();
                dataSource.setUrl(url);
                dataSource.setUsername(username);
                dataSource.setPassword(password);
                dataSource.setLoginTimeout(5);
                dataSource.setCheckExecuteTime(true);
                dataSource.setMaxWait(TimeUnit.SECONDS.toMillis(10));
                return dataSource;
            }
        }).getConnection();

    }

    public default boolean existTable(Connection connection, String db, String table) throws Exception {
        return !executeQuery(connection, String.format("SHOW TABLES from %s LIKE '%s';", db, table)).isEmpty();

    }

    public default boolean hasData(Connection connection, String db, String table) throws Exception {
        return !executeQuery(connection, String.format("select * from %s.%s limit 1", db, table)).isEmpty();
    }

    public default void deleteData(Connection connection, String db, String table) throws Exception {
        execute(connection, String.format("delete  from %s.%s", db, table));
    }

    public default void execute(Connection mySQLConnection, String sql) throws Exception {
        LOGGER.info(sql);
        JdbcUtils.execute(mySQLConnection, sql);
    }

    public default List<Map<String, Object>> executeQuery(Connection mySQLConnection, String sql) throws Exception {
        LOGGER.info(sql);
        return JdbcUtils.executeQuery(mySQLConnection, sql, Collections.emptyList());
    }
    public default void addC0(Connection connection) throws Exception {
        execute(connection, CreateDataSourceHint
                .create("newDs",
                        DB1));
        execute(connection, CreateClusterHint.create("c0", Arrays.asList("newDs"), Collections.emptyList()));
    }
}

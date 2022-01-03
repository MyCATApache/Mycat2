package io.mycat.assemble;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


public interface MycatTest {

    String DB_MYCAT = System.getProperty("db_mycat", "jdbc:mysql://localhost:8066/mysql?username=root&password=123456&useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true");
    String DB1 = System.getProperty("db1", "jdbc:mysql://localhost:3306/mysql?username=root&password=123456&useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true");
    String DB2 = System.getProperty("db2", "jdbc:mysql://localhost:3307/mysql?username=root&password=123456&useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true");
    String DB_MYCAT_PSTMT = System.getProperty("db_mycat", "jdbc:mysql://localhost:8066/mysql?username=root&password=123456&useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true&useServerPrepStmts=true");

    String RESET_CONFIG = "/*+ mycat:resetConfig{} */";

    Map<String, DruidDataSource> dsMap = new ConcurrentHashMap<>();
    Logger LOGGER = LoggerFactory.getLogger(MycatTest.class);


    default Connection getMySQLConnection(String url) throws Exception {
        return dsMap.computeIfAbsent(url, new Function<String, DruidDataSource>() {
            @Override
            @SneakyThrows
            public DruidDataSource apply(String url) {
                Map<String, String> urlParameters = JsonUtil.urlSplit(url);
                String username = urlParameters.getOrDefault("username", "root");
                String password = urlParameters.getOrDefault("password", "123456");

                DruidDataSource dataSource = new DruidDataSource();
                dataSource.setUrl(url);
                dataSource.setUsername(username);
                dataSource.setPassword(password);

                dataSource.setLoginTimeout(100000);
                dataSource.setCheckExecuteTime(true);
                dataSource.setQueryTimeout(100);
                dataSource.setMaxWait(TimeUnit.SECONDS.toMillis(100));
                dataSource.setMaxActive(8);
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

    public default long count(Connection connection, String db, String table) throws Exception {
        String format = String.format("select count(1) as `count` from %s.%s", db, table);
        List<Map<String, Object>> mapList = executeQuery(connection, format);
        Number count = (Number) mapList.get(0).get("count");
        return count.longValue();
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
    public default String executeQueryAsText(Connection mySQLConnection, String sql) throws Exception {
        LOGGER.info(sql);
        return JdbcUtils.executeQuery(mySQLConnection, sql, Collections.emptyList()).toString();
    }

    public default String explain(Connection mySQLConnection, String sql) throws Exception {
        List<Map<String, Object>> maps = JdbcUtils.executeQuery(mySQLConnection, "explain "+sql, Collections.emptyList());
        return maps.stream().flatMap(i -> i.values().stream()).map(i -> (String) i).collect(Collectors.joining("\n"));
    }

    public default void addC0(Connection connection) throws Exception {
        execute(connection, CreateDataSourceHint
                .create("newDs",
                        DB1));
        execute(connection, CreateClusterHint.create("c0", Arrays.asList("newDs"), Collections.emptyList()));
    }


    public default MycatRowMetaData getColumns(Connection connection, String db, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from " + db + "." + table)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            return new CopyMycatRowMetaData(new JdbcRowMetaData(metaData));
        }
    }
}

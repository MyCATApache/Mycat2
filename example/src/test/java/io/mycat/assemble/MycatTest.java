package io.mycat.assemble;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.mycat.MycatCore;
import io.mycat.example.MycatRunner;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import lombok.SneakyThrows;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


public interface MycatTest {

    String RESET_CONFIG ="/*+ mycat:resetConfig{} */";

    final Map<Integer, DruidDataSource> dsMap = new ConcurrentHashMap<>();



    default Connection getMySQLConnection(int port) throws Exception {
        MycatRunner.checkRunMycat();
        return dsMap.computeIfAbsent(port, new Function<Integer, DruidDataSource>() {
            @Override
            @SneakyThrows
            public DruidDataSource apply(Integer integer) {
                String username = "root";
                String password = "123456";
                DruidDataSource dataSource = new DruidDataSource();
                dataSource.setUrl("jdbc:mysql://127.0.0.1:" +
                        port + "/?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
                dataSource.setUsername(username);
                dataSource.setPassword(password);
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
        System.out.println(sql);
        JdbcUtils.execute(mySQLConnection, sql);
    }

    public default List<Map<String, Object>> executeQuery(Connection mySQLConnection, String sql) throws Exception {
        System.out.println(sql);
        return JdbcUtils.executeQuery(mySQLConnection, sql, Collections.emptyList());
    }
    public default void addC0(Connection connection) throws Exception {
        execute(connection, CreateDataSourceHint
                .create("newDs",
                        "jdbc:mysql://127.0.0.1:3306"));
        execute(connection, CreateClusterHint.create("c0", Arrays.asList("newDs"), Collections.emptyList()));
    }
}

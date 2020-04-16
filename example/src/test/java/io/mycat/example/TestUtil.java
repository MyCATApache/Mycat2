package io.mycat.example;

import com.mysql.cj.jdbc.MysqlDataSource;
import io.mycat.hbt.TextConvertor;
import lombok.SneakyThrows;
import org.mariadb.jdbc.MariaDbDataSource;

import java.sql.*;
import java.util.Properties;

public class TestUtil {

    @SneakyThrows
    public static Connection getMySQLConnection() {
        String username = "root";
        String password = "123456";

        Properties properties = new Properties();
        properties.put("user", username);
        properties.put("password", password);
        properties.put("useBatchMultiSend", "false");
        properties.put("usePipelineAuth", "false");

        String url = "jdbc:mysql://0.0.0.0:8067/db1?useServerPrepStmts=false&useCursorFetch=false&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8";

        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setUrl(url);
        mysqlDataSource.setUser(username);
        mysqlDataSource.setPassword(password);

        return mysqlDataSource.getConnection();
    }

    @SneakyThrows
    public static Connection getMariaDBConnection() {
        String username = "root";
        String password = "123456";

        Properties properties = new Properties();
        properties.put("user", username);
        properties.put("password", password);
        properties.put("useBatchMultiSend", "false");
        properties.put("usePipelineAuth", "false");

        String url = "jdbc:mysql://0.0.0.0:8067/db1?useServerPrepStmts=false&useCursorFetch=false&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8";

        MariaDbDataSource mysqlDataSource = new MariaDbDataSource();
        mysqlDataSource.setUrl(url);
        mysqlDataSource.setUser(username);
        mysqlDataSource.setPassword(password);

        return mysqlDataSource.getConnection();
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        String username = "root";
        String password = "123456";

        Properties properties = new Properties();
        properties.put("user", username);
        properties.put("password", password);
        properties.put("useBatchMultiSend", "false");
        properties.put("usePipelineAuth", "false");

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://0.0.0.0:8066/scott?useServerPrepStmts=false&useCursorFetch=true&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8", properties);
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select 1");

            String s = getString(resultSet);
            System.out.println(s);
        }
        connection.setAutoCommit(false);
        Statement statement1 = connection.createStatement();
        boolean execute = statement1.execute("INSERT INTO `db1`.`travelrecord` (`id`, `d`) VALUES (1, X'89'); ", Statement.RETURN_GENERATED_KEYS);
        connection.rollback();

    }

    public static String getString(ResultSet resultSet) throws SQLException {
        return TextConvertor.dumpResultSet(resultSet).replaceAll("\r","").replaceAll("\n","");
    }

}
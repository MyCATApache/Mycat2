package io.mycat.mycat2.e2e;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class BaseSQLTest {
    //    final static int port = 3306;
    final static int port = 8066;
    final static String URL = "jdbc:mysql://127.0.0.1:" + port + "/db1?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC" +
            "&useLocalSessionState=true&failOverReadOnly=false" +
            "&rewriteBatchedStatements=true" +
            "&allowMultiQueries=true" +
            "&useCursorFetch=true";
    final static String USERNAME = "root";
    final static String PASSWORD = "123456";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void using(ConsumerIO<Connection> c) {
        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            c.accept(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Connection newConnection() throws SQLException {
        return DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }

    @FunctionalInterface
    interface ConsumerIO<T> {
        void accept(T t) throws Exception;
    }
}

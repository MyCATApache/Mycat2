package io.mycat.mycat2.e2e;

import org.junit.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * cjw
 * <p>
 * create table travelrecord (id bigint not null primary key,user_id varchar(100),traveldate DATE, fee decimal,days int);
 */
public class BaseSQLExeTest {

    final static String URL = "jdbc:mysql://127.0.0.1:8066/db1?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC";
    final static String USERNAME = "root";
    final static String PASSWORD = "123456";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testOneNormalSQl() {
        using(c -> {
                    Statement statement = c.createStatement();
                    while (true) {
                        statement.executeQuery("SELECT * FROM `db1`.`travelrecord`;");
                    }
                }
        );
    }

    /**
     *
     */
    public static void testTransaction() {
        using(c -> {
                    c.createStatement().executeUpdate("DELETE FROM `db1`.`travelrecord` WHERE `id` = '1'; ");
                    c.createStatement().executeUpdate("INSERT INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`) VALUES ('1', '2', '2018-11-02', '2', '2'); ");
                    c.setAutoCommit(false);

                    c.createStatement().executeUpdate("DELETE FROM `db1`.`travelrecord` WHERE `id` = '1'; ");
                    c.rollback();

                    ResultSet resultSet = c.createStatement().executeQuery("SELECT * FROM `db1`.`travelrecord`;");
                    Assert.assertTrue(resultSet.next());
                }
        );
    }

    public static void testStoredProcedure() {
        using(c -> {
                    c.createStatement().executeUpdate("CREATE TEMPORARY TABLE ins ( id INT );");
                    c.createStatement().executeUpdate("DROP PROCEDURE IF EXISTS multi;");
                    c.createStatement().executeUpdate(
                            "CREATE PROCEDURE multi()" +
                            "SELECT 1;" +
                            "SELECT 1;" +
                            "INSERT INTO ins VALUES (1);" +
                            "INSERT INTO ins VALUES (2);" +
                            "INSERT INTO ins VALUES (3);"
                    );
            ResultSet resultSet = c.createStatement().executeQuery("CALL multi();");
            Assert.assertTrue(resultSet.next());
            c.createStatement().executeUpdate("DROP TABLE ins;");
                }
        );
    }
    public static void main(String[] args) {
        testStoredProcedure();
    }

    public static void using(ConsumerIO<Connection> c) {
        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            c.accept(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @FunctionalInterface
    interface ConsumerIO<T> {
        void accept(T t) throws Exception;
    }

}

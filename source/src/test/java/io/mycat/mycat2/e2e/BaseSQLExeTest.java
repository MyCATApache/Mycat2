package io.mycat.mycat2.e2e;

import org.junit.Assert;

import java.sql.*;

/**
 * cjw
 * <p>
 * create table travelrecord (id bigint not null primary key,user_id varchar(100),traveldate DATE, fee decimal,days int);
 */
public class BaseSQLExeTest {
    //3306
    //8066
    final static String URL = "jdbc:mysql://127.0.0.1:8066/db1?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC" +
            "&useLocalSessionState=true&failOverReadOnly=false" +
            "&rewriteBatchedStatements=true" +
            "&allowMultiQueries=true" +
            "&useCursorFetch=true"+
            "&useSSL=false";
    final static String USERNAME = "root";
    final static String PASSWORD = "123456";
    final static boolean LOCAL = false;

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testOneNormalSQl() {
        using(c -> {
                    int i = 0;
                    Statement statement = c.createStatement();
                    while (i < 100) {
                        statement.executeQuery("SELECT * FROM `db1`.`travelrecord`;");
                        i++;
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

    public static void testStoredProcedure2() {
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
                    CallableStatement callableStatement = c.prepareCall("CALL multi();");
                    ResultSet resultSet = callableStatement.executeQuery();
                    Assert.assertTrue(resultSet.next());


                    c.createStatement().executeUpdate("DROP TABLE ins;");
                }
        );
    }

    public static void testPreparedStatement() {
        using(c -> {
                    c.createStatement().executeUpdate("DELETE FROM `db1`.`travelrecord` WHERE `id` = '1'; ");
                    c.createStatement().executeUpdate("INSERT INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`) VALUES ('1', '2', '2018-11-02', '2', '2'); ");

                    PreparedStatement preparedStatement = c.prepareStatement("SELECT * FROM `db1`.`travelrecord` WHERE id = ?");
                    preparedStatement.setInt(1, 1);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    Assert.assertTrue(resultSet.next());
                }
        );
    }

    public static void testRewriteBatchedStatements() {
        using(c -> {
                    c.createStatement().executeUpdate("truncate table `db1`.`travelrecord`");
                    int size = 8;
                    PreparedStatement statement = c.prepareStatement("INSERT INTO `db1`.`travelrecord` VALUES (?, '2', '2018-11-02', '2', '2')");
                    for (int i = 0; i < size; i++) {
                        statement.setString(1, i + "");
                        statement.addBatch();
                    }
                    int[] results = statement.executeBatch();
                    for (int i : results) {
                        Assert.assertEquals(1, i);
                    }
                }
        );
    }

    public static void testCursor() {
        using(c -> {
                    c.createStatement().executeUpdate("truncate table `db1`.`travelrecord`");
                    int size = 2;
                    PreparedStatement statement = c.prepareStatement("INSERT INTO `db1`.`travelrecord` VALUES (?, '2', '2018-11-02', '2', '2')");
                    for (int i = 0; i < size; i++) {
                        statement.setString(1, i + "");
                        statement.addBatch();
                    }
                    int[] results = statement.executeBatch();

                    PreparedStatement statement2 = c.prepareStatement("SELECT * FROM `db1`.`travelrecord`;", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    statement2.setFetchSize(10);
                    ResultSet resultSet = statement2.executeQuery();

                    Assert.assertTrue(resultSet.next());
                    Assert.assertTrue(resultSet.next());
                }
        );
    }

    public static void testFieldList() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW COLUMNS FROM db1.`travelrecord`;");
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.next());
            Assert.assertTrue(resultSet.next());
        });
    }

    public static void main(String[] args) {
//        testOneNormalSQl();
//        testTransaction();
//        testPreparedStatement();
//        testStoredProcedure();
//        testStoredProcedure2();
//        testRewriteBatchedStatements();
//        testCursor();
        testFieldList();
    }

    public static void using(ConsumerIO<Connection> c) {
        if (LOCAL){
            try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
                c.accept(connection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @FunctionalInterface
    public interface ConsumerIO<T> {
        void accept(T t) throws Exception;
    }

}

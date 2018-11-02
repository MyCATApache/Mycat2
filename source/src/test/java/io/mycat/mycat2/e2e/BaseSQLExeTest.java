package io.mycat.mycat2.e2e;

import java.sql.Connection;
import java.sql.DriverManager;
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

    public static void main(String[] args) {
        testOneNormalSQl();
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

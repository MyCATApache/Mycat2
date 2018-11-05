package io.mycat.mycat2.e2e.comQuery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by linxiaofang on 2018/11/5.
 * create database db1;
 * create table travelrecord (id bigint not null primary key,user_id varchar(100),traveldate DATE, fee decimal,days int);
 */
public class ComQueryTest {
    //3306
    //8066
    final static String URL = "jdbc:mysql://127.0.0.1:3306/db1?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC" +
            "&useLocalSessionState=true&failOverReadOnly=false" +
            "&rewriteBatchedStatements=true" +
            "&allowMultiQueries=true" +
            "&useCursorFetch=true";
    final static String USERNAME = "root";
    final static String PASSWORD = "";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testShowTableStatus() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.executeQuery("SHOW TABLE STATUS;");
                    statement.executeQuery("SHOW TABLE STATUS FROM db1;");
                    statement.executeQuery("SHOW TABLE STATUS IN db1;");
                    statement.executeQuery("SHOW TABLE STATUS LIKE 'travel%';");
                    statement.executeQuery("SHOW TABLE STATUS WHERE Name LIKE 'travel%';");
                    statement.executeQuery("SHOW TABLE STATUS FROM db1 WHERE Name LIKE 'travel%';");
                    statement.executeQuery("SHOW TABLE STATUS IN db1 WHERE Engine='InnoDB';");
                }
        );
    }

    public static void testShowTriggers() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.executeQuery("SHOW TRIGGERS;");
                    statement.executeQuery("SHOW TRIGGERS FROM db1;");
                    statement.executeQuery("SHOW TRIGGERS IN db1;");
                    statement.executeQuery("SHOW TRIGGERS LIKE 'acc%';");
                    statement.executeQuery("SHOW TRIGGERS WHERE `Trigger` LIKE 'acc%';");
                    statement.executeQuery("SHOW TRIGGERS FROM db1 WHERE `Trigger` LIKE 'acc%';");
                    statement.executeQuery("SHOW TRIGGERS IN db1 WHERE `Trigger` LIKE 'acc%';");
                }
        );
    }


    public static void main(String[] args) {
        testShowTableStatus();
        testShowTriggers();
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

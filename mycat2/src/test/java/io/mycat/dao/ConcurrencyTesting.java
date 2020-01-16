package io.mycat.dao;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

import static io.mycat.dao.TestUtil.getMySQLConnection;

public class ConcurrencyTesting {
    public static void main(String[] args) {
        AtomicLong counter = new AtomicLong(0);

       for (int i = 0;i<10;i++){
            new Thread(() -> {
                try {
                    while (true) {
                        try (Connection mySQLConnection = getMySQLConnection()) {
                            mySQLConnection.setAutoCommit(false);
                            try (Statement statement = mySQLConnection.createStatement()) {
                                statement.executeUpdate(String.format(
                                        "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('%s'); ",
                                        Long.valueOf(counter.getAndIncrement()).toString()));
                            }
                            mySQLConnection.commit();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }


    }
}
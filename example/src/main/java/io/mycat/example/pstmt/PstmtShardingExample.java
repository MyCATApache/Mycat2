package io.mycat.example.pstmt;

import io.mycat.example.ExampleObject;
import io.mycat.example.TestUtil;
import io.mycat.hbt.TextConvertor;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class PstmtShardingExample  extends ExampleObject {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        main(args, PstmtShardingExample.class);
    }

    @Test
    public void testWrapper() throws Exception {
        main(new String[]{null,null,"test","server"});
    }


    public static void test() throws Exception {
        try (Connection mySQLConnection = TestUtil.getPstmtMySQLConnection()) {
            try(Statement statement1 = mySQLConnection.createStatement()){
                statement1.execute("delete `db1`.`travelrecord`");
            }
            PreparedStatement preparedStatement = mySQLConnection.prepareStatement("INSERT INTO `db1`.`travelrecord` (`id`,`blob`) VALUES (?,?)");
            StringBuilder sb = new StringBuilder();
            IntStream.rangeClosed(0,8192).forEach(i->sb.append(i));
            byte[] blob = sb.toString().getBytes();
            for (int i = 1; i <= 600; i++) {
                preparedStatement.setInt(1,i);
                preparedStatement.setBytes(2,blob);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            Statement statement = mySQLConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select count(*) from db1.travelrecord");
            long aLong = 0;
            while (resultSet.next()){
                aLong= resultSet.getLong(1);
            }
            Assert.assertEquals(600,aLong);
        }
        try (Connection mySQLConnection = TestUtil.getPstmtMySQLConnection()) {
            try(Statement statement1 = mySQLConnection.createStatement()){
                statement1.execute("delete `db1`.`travelrecord`");
            }
            PreparedStatement preparedStatement = mySQLConnection.prepareStatement("INSERT INTO `db1`.`travelrecord` (`id`,`user_id`) VALUES (?,?)");
            ThreadLocalRandom current = ThreadLocalRandom.current();
            for (int i = 1; i <= 600; i++) {
                preparedStatement.setInt(1,i);
                preparedStatement.setInt(2,current.nextInt());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            Statement statement = mySQLConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("select count(*) from db1.travelrecord");
            long aLong = 0;
            while (resultSet.next()){
                aLong= resultSet.getLong(1);
            }
            Assert.assertEquals(600,aLong);
        }
        try (Connection mySQLConnection = TestUtil.getPstmtMySQLConnection()) {
            PreparedStatement preparedStatement = mySQLConnection.prepareStatement("select * from db1.travelrecord where id =? and user_id = ?");
            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 2);
            String s = TextConvertor.dumpResultSet(preparedStatement.executeQuery());
            System.out.println(s);
        }
        try (Connection mySQLConnection = TestUtil.getPstmtMySQLConnection()) {
            PreparedStatement preparedStatement = mySQLConnection.prepareStatement("select 1");
            String s = TextConvertor.dumpResultSet(preparedStatement.executeQuery());
            System.out.println(s);
        }
    }
}
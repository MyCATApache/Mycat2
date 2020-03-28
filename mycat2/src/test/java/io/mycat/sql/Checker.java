package io.mycat.sql;

import io.mycat.dao.TestUtil;
import io.mycat.hbt.TextConvertor;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class Checker {
    @SneakyThrows
    public static void main(String[] args) {
        List<String> initList = Arrays.asList("set xa = off");

        try(Connection mySQLConnection = TestUtil.getMySQLConnection()){
            try(Statement statement = mySQLConnection.createStatement()){
                ResultSet resultSet = statement.executeQuery("select 1 from db1.travelrecord where id = 1 limit 1");
                String s = TextConvertor.dumpResultSet(resultSet);
                System.out.println(s);
            }
        }

    }
}
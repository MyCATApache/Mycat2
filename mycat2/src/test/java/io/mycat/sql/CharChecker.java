package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class CharChecker  extends BaseChecker{
    public CharChecker(Statement statement) {
        super(statement);
    }
    @Override
    public void run() {
        simplyCheck("ASCII('a')", "(97)");
//        simplyCheck("CONCAT('a','b')", "1");
//        simplyCheck("CONCAT_WS('a','b')", "1");
//        simplyCheck("BIN('a')", "1");
//        simplyCheck("BIT_LENGTH('a')", "1");
//        simplyCheck("CHAR('a')", "1");
//        simplyCheck("CHAR_LENGTH('a')", "1");
//        simplyCheck("CHARACTER_LENGTH('a')", "1");
    }


    @SneakyThrows
    public static void main(String[] args) {
        List<String> initList = Arrays.asList("set xa = off");
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            for (String u : initList) {
                statement.execute(u);
            }
            BaseChecker checker = new CharChecker(statement);
            checker.run();
        }
    }
}
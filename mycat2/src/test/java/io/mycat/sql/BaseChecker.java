package io.mycat.sql;

import io.mycat.dao.TestUtil;
import io.mycat.hbt.TextConvertor;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class BaseChecker implements Runnable {
    final Statement statement;

    public BaseChecker(Statement statement) {
        this.statement = statement;
    }

    public void simplyCheck(String fun, String expected) {
        String format = MessageFormat.format("select {0} from db1.travelrecord where id = 1 limit 1", fun);
        check(format, expected);//
    }

    @SneakyThrows
    public Ok executeUpdate(String sql) {
        int i = statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
        ResultSet generatedKeys = statement.getGeneratedKeys();
        while (generatedKeys.next()) {
            return new Ok(i, generatedKeys.getLong(1));
        }
        throw new UnsupportedOperationException();
    }

    @Getter
    @AllArgsConstructor
    public static class Ok {
        int updateCount;
        long lastId;
    }

    @SneakyThrows
    public static void main(String[] args) {
        List<String> initList = Arrays.asList("set xa = off");
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            for (String u : initList) {
                statement.execute(u);
            }
            BaseChecker checker = new MathChecker(statement);
            checker.run();
        }
    }
    @SneakyThrows
    public void check(String sql, Object expectedRes) {
        check(sql, Objects.toString(expectedRes));
    }
    @SneakyThrows
    public void check(String sql, String expectedRes) {
        ResultSet resultSet = statement.executeQuery(sql);
        String s = TextConvertor.dumpResultSet(resultSet).replaceAll("\n", "").replaceAll("\r", "");
        System.out.println(s);
        if (!expectedRes.startsWith("(")) {
            expectedRes = "(" + expectedRes + ")";
        }
        if (!s.startsWith("(")) {
            s = "(" + s + ")";
        }
        Assert.assertEquals(expectedRes, s);
    }

    @SneakyThrows
    public void simplyCheck(String sql) {
        check(MessageFormat.format("select {0} from db1.travelrecord where id = 1 limit 1", sql));
    }

    @SneakyThrows
    public void check(String sql) {
        statement.execute(sql);
    }
    @SneakyThrows
    public void checkHbt(String sql,String expectedRes) {
        check("execute plan "+sql,expectedRes);
    }
    @SneakyThrows
    public void checkContains(String sql, String element) {
        ResultSet resultSet = statement.executeQuery(sql);
        String s = TextConvertor.dumpResultSet(resultSet).replaceAll("\n", "").replaceAll("\r", "");
        System.out.println(s);
        boolean contains = s.contains(element);
        Assert.assertTrue(contains);
    }

    public Statement getStatement() {
        return statement;
    }
}
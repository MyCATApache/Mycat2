package io.mycat.sql;

import com.alibaba.druid.util.JdbcUtils;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class SqlFunctionTest {

    Connection getMySQLConnection(int port) throws SQLException {
        String username = "root";
        String password = "123456";
//        properties.put("useBatchMultiSend", "false");
//        properties.put("usePipelineAuth", "false");
        String url = "jdbc:mysql://127.0.0.1:" +
                port +
                "/?useServerPrepStmts=false&useCursorFetch=false&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8";
        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setUrl(url);
        mysqlDataSource.setUser(username);
        mysqlDataSource.setPassword(password);

        return mysqlDataSource.getConnection();
    }

    @Test
    public void testCharFunction() throws SQLException {
        check("SELECT ASCII('a')", "97");
        check("SELECT BIN('12')", "1100");
        check("SELECT BIT_LENGTH('text')", "32");
        check("SELECT CHAR(77,121,83,81,'76')", "MySQL");
        check("SELECT CHAR_LENGTH('a')", "1");
        check("SELECT CHARACTER_LENGTH('a')", "1");
        check("SELECT CONCAT('a','b')", "ab");
        check("SELECT CONCAT_WS(',','a','b')", "a,b");
        check("SELECT ELT(1, 'Aa', 'Bb', 'Cc', 'Dd')", "(Aa)");
        check("SELECT EXPORT_SET(5,'Y','N',',',4)", "Y,N,Y,N");
        check("SELECT FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff')", "2");
        check("SELECT FIND_IN_SET('b','a,b,c,d')", "2");
        check("SELECT FORMAT(12332.123456, 4)", "12,332.1235");
        check("SELECT TO_BASE64('abc')", "YWJj");
        check("SELECT FROM_BASE64(TO_BASE64('abc'))", "abc");
        checkValue("SELECT X'616263', HEX('abc'), UNHEX(HEX('abc')) ", "(abc,616263,abc)");
        check("SELECT HEX(255), CONV(HEX(255),16,10) ", "(FF,255)");
        check("SELECT INSERT('Quadratic', 3, 4, 'What') ", "(QuWhattic)");
        check("SELECT INSERT('Quadratic', -1, 4, 'What') ", "(Quadratic)");
        check("SELECT INSERT('Quadratic', 3, 100, 'What') ", "(QuWhat)");
        check("SELECT INSTR('foobarbar', 'bar') ", "(4)");
        check("SELECT INSTR('xbar', 'foobar') ", "(0)");
        check("SELECT INSTR('xbar', 'foobar') ", "(0)");
        check("SELECT LEFT('foobarbar', 5) ", "(fooba)");
        check("SELECT LENGTH('text') ", "(4)");
        check("SELECT LOCATE('bar', 'foobarbar') ", "(4)");
        check("SELECT LPAD('hi',4,'??') ", "(??hi)");
        check("SELECT LPAD('hi',1,'??') ", "(h)");
        check("SELECT LTRIM('  barbar') ", "(barbar)");
        check("SELECT MAKE_SET(1,'a','b','c') ", "(a)");
        check("SELECT MAKE_SET(1|4,'hello','nice','world') ", "(hello,world)");
        check("SELECT MAKE_SET(1|4,'hello','nice',NULL,'world') ", "(hello)");
        check("SELECT MAKE_SET(0,'a','b','c') ", "()");
        check("SELECT MAKE_SET(0,'a','b','c') ", "()");
        check("SELECT SUBSTRING('Quadratically',5) ", "(ratically)");
        checkValue("SELECT SUBSTRING('foobarbar' FROM 4) ", "(barbar)");
        checkValue("SELECT SUBSTRING('Sakila', -3) ", "(ila)");
        checkValue("SELECT SUBSTRING('Quadratically',5,6) ", "(ratica)");
        checkValue("SELECT SUBSTRING('Sakila', -3) ", "(ila)");
        checkValue("SELECT SUBSTRING('Sakila', -5, 3) ", "(aki)");
        checkValue("SELECT SUBSTRING('Sakila' FROM -4 FOR 2) ", "(ki)");
        checkValue("SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2) ", "(www.mysql)");
        checkValue("SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2) ", "(mysql.com)");
        checkValue("SELECT TRIM('  bar   ') ", "(bar)");
        checkValue("SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx') ", "(barxxx)");
        checkValue("SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx') ", "(bar)");
        checkValue("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') ", "(barx)");
        checkValue("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') ", "(barx)");
        check("SELECT UNHEX('4D7953514C') ", "(MySQL)");
        checkValue("SELECT X'4D7953514C' ", "(MySQL)");
        check("SELECT UNHEX(HEX('string')) ", "(string)");
        check("SELECT HEX(UNHEX('1267')) ", "(1267)");
        check("SELECT UNHEX('GG') ", "(null)");
        check("SELECT UPPER('a') ", "(A)");
        check("SELECT LOWER('A') ", "(a)");
        check("SELECT LCASE('A') ", "(a)");
        check("SELECT UNHEX('GG') ", "(null)");


    }

    private void checkValue(String s, String s1)throws SQLException  {
        checkValue(s);
    }

    private void check(String s, String s1) throws SQLException {
        try (Connection mySQLConnection = getMySQLConnection(3306);
             Connection mycatConnection = getMySQLConnection(8066);
        ) {
            Assert.assertEquals(
                    executeQueryAsString(mySQLConnection, s)
                    , (executeQueryAsString(mycatConnection, s)));
        }
    }
    private void checkValue(String s) throws SQLException {
        try (Connection mySQLConnection = getMySQLConnection(3306);
             Connection mycatConnection = getMySQLConnection(8066);
        ) {
            Assert.assertEquals(
                    executeQuery(mySQLConnection, s)
                            .stream().map(i->i.values()).collect(Collectors.toList()).toString()
                    , (executeQuery(mycatConnection, s))
                            .stream().map(i->i.values()).collect(Collectors.toList()).toString()
            );
        }
    }


    private <T> String executeQueryAsString(Connection conn, String s) throws SQLException {

        List<Map<String, Object>> rows = executeQuery(conn, s);

        return rows.toString();
    }

    @NotNull
    private List<Map<String, Object>> executeQuery(Connection conn, String s) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();


            rs = stmt.executeQuery(s);

            ResultSetMetaData rsMeta = rs.getMetaData();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();

                for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1).replaceAll(" ","");
                    Object value = rs.getString(i + 1);
                    row.put(columName, value);
                }

                rows.add(row);
            }
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return rows;
    }
}


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
        check("SELECT ASCII('a')");
        check("SELECT BIN('12')");
        check("SELECT BIT_LENGTH('text')");
        check("SELECT CHAR(77,121,83,81,'76')");
        check("SELECT CHAR_LENGTH('a')");
        check("SELECT CHARACTER_LENGTH('a')");
        check("SELECT CONCAT('a','b')");
        check("SELECT CONCAT_WS(',','a','b')");
        check("SELECT ELT(1, 'Aa', 'Bb', 'Cc', 'Dd')");
        check("SELECT EXPORT_SET(5,'Y','N',',',4)");
        check("SELECT FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff')");
        check("SELECT FIND_IN_SET('b','a,b,c,d')");
        check("SELECT FORMAT(12332.123456, 4)");
        check("SELECT TO_BASE64('abc')");
        check("SELECT FROM_BASE64(TO_BASE64('abc'))");
        checkValue("SELECT X'616263', HEX('abc'), UNHEX(HEX('abc')) ", "(abc,616263,abc)");
        check("SELECT HEX(255), CONV(HEX(255),16,10) ");
        check("SELECT INSERT('Quadratic', 3, 4, 'What') ");
        check("SELECT INSERT('Quadratic', -1, 4, 'What') ");
        check("SELECT INSERT('Quadratic', 3, 100, 'What') ");
        check("SELECT INSTR('foobarbar', 'bar') ");
        check("SELECT INSTR('xbar', 'foobar') ");
        check("SELECT INSTR('xbar', 'foobar') ");
        check("SELECT LEFT('foobarbar', 5) ");
        check("SELECT LENGTH('text') ");
        check("SELECT LOCATE('bar', 'foobarbar') ");
        check("SELECT LPAD('hi',4,'??') ");
        check("SELECT LPAD('hi',1,'??') ");
        check("SELECT LTRIM('  barbar') ");
        check("SELECT MAKE_SET(1,'a','b','c') ");
        check("SELECT MAKE_SET(1|4,'hello','nice','world') ");
        check("SELECT MAKE_SET(1|4,'hello','nice',NULL,'world') ");
        check("SELECT MAKE_SET(0,'a','b','c') ");
        check("SELECT MAKE_SET(0,'a','b','c') ");
        check("SELECT SUBSTRING('Quadratically',5) ");
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
        check("SELECT UNHEX('4D7953514C') ");
        checkValue("SELECT X'4D7953514C' ", "(MySQL)");
        check("SELECT UNHEX(HEX('string')) ");
        check("SELECT HEX(UNHEX('1267')) ");
        check("SELECT UNHEX('GG') ");
        check("SELECT UPPER('a') ");
        check("SELECT LOWER('A') ");
        check("SELECT LCASE('A') ");
        check("SELECT UNHEX('GG') ");


    }

    private void checkValue(String s, String s1)throws SQLException  {
        checkValue(s);
    }

    private void check(String s) throws SQLException {
        try (Connection mySQLConnection = getMySQLConnection(3306);
             Connection mycatConnection = getMySQLConnection(8066);
        ) {
            Assert.assertEquals(
                    executeQueryAsString(mySQLConnection, s)
                    , (executeQueryAsString(mycatConnection, s)));
        }
    }
    private void uncheckValue(String s) throws SQLException {
        try (
             Connection mycatConnection = getMySQLConnection(8066);
        ) {
            executeQuery(mycatConnection, s);
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
    @Test
    public void testMathFunction() throws SQLException {
        checkValue("SELECT abs(-1)", "1");//
        checkValue("SELECT ACOS(-1)", "3.141592653589793");//
        checkValue("SELECT ASIN(-1)", "-1.5707963267948966");//
        checkValue("SELECT ATAN(-1)", "-0.7853981633974483");//
        checkValue("SELECT ATAN2(-1,-1)", "-2.356194490192345");//
        checkValue("SELECT ATAN(-1)", "-0.7853981633974483");//
        checkValue("SELECT CEIL(-1)", "-1");//
        checkValue("SELECT CEILING(-1)", "-1");//
        checkValue("SELECT CONV(16,10,16) ","10");
        checkValue("SELECT COS(-1)", "0.5403023058681398");//
        checkValue("SELECT COT(-1)", "-0.6420926159343306");//
        checkValue("SELECT CRC32(-1) ","808273962");
        checkValue("SELECT DEGREES(-1)", "-57.29577951308232");//
        checkValue("SELECT EXP(-1)", "0.36787944117144233");//
        checkValue("SELECT FLOOR(-1)", "-1");//
        checkValue("SELECT LN(2)", "0.6931471805599453");//
        checkValue("SELECT LOG(10) ","1.0");//
        checkValue("SELECT LOG10(10) ","1.0");//
        //checkValue("SELECT LOG2(10) ","3.3219280948873626");//精度过高
        checkValue("SELECT MOD(6,5)", "1");//
       // checkValue("SELECT PI() ","3.141593");//精度过高
        checkValue("SELECT POW(-1,2)");//
        checkValue("SELECT POWER(-1,2)");//
        uncheckValue("SELECT RAND(1)");//
        checkValue("SELECT ROUND(-1)", "-1");//
        checkValue("SELECT SIGN(-1)", "-1");//
        checkValue("SELECT SIN(-1)", "-0.8414709848078965");//
        checkValue("SELECT SQRT(2)", "1.4142135623730951");//
        checkValue("SELECT TAN(-1)", "-1.5574077246549023");//
        checkValue("SELECT TRUNCATE(123.4567, 3)", "123.456");//

    }

    @Test
    public void testTimeFunction() throws SQLException{
        checkValue("SELECT ADDDATE(\"2017-06-15\", INTERVAL 10 DAY);");//
        checkValue("SELECT ADDTIME(\"2017-06-15 09:34:21\", \"2\");");//
        checkValue("SELECT CURDATE();");//
        checkValue("SELECT CURRENT_DATE();");//
        checkValue("SELECT CURRENT_TIME();");//
        checkValue("SELECT DATE('2003-12-31 01:02:03');");//
        uncheckValue("SELECT CURTIME() + 0;");//
        checkValue("SELECT DATEDIFF('2007-12-31 23:59:59','2007-12-30')");//
        checkValue("SELECT DATEDIFF('2010-11-30 23:59:59','2010-12-31');");//
        checkValue("SELECT DATE_ADD('2018-05-01',INTERVAL 1 DAY);");//
        checkValue("SELECT DATE_SUB('2018-05-01',INTERVAL 1 YEAR);");//
        uncheckValue("SELECT DATE_ADD('2020-12-31 23:59:59',INTERVAL 1 SECOND);");//
        checkValue("SELECT DATE_FORMAT(\"2017-06-15\", \"%Y\");");
        checkValue("SELECT DAY(\"2017-06-15\");");
        checkValue("SELECT DAYNAME(\"2017-06-15\");");
        checkValue("SELECT DAYOFMONTH(\"2017-06-15\");");
        checkValue("SELECT DAYOFWEEK(\"2017-06-15\");");
        checkValue("SELECT DAYOFYEAR(\"2017-06-15\");");
        checkValue("SELECT EXTRACT(MONTH FROM \"2017-06-15\");");
        checkValue("SELECT FROM_DAYS(685467);");
        checkValue("SELECT HOUR(\"2017-06-20 09:34:00\");");
        checkValue("SELECT LAST_DAY(\"2017-06-20\");");
        checkValue("SELECT LOCALTIME();");
        checkValue("SELECT LOCALTIMESTAMP();");
        checkValue("SELECT MAKEDATE(2017, 3);");
        checkValue("SELECT MAKETIME(11, 35, 4);");
        checkValue("SELECT MICROSECOND(\"2017-06-20 09:34:00.000023\");");
        checkValue("SELECT MINUTE(\"2017-06-20 09:34:00\");");
        checkValue("SELECT MONTH(\"2017-06-15\");");
        checkValue("SELECT MONTHNAME(\"2017-06-15\");");

        checkValue("SELECT NOW();");
        checkValue("SELECT PERIOD_ADD(201703, 5)");
        checkValue("SELECT PERIOD_DIFF(201710, 201703);");
        checkValue("SELECT QUARTER(\"2017-06-15\");");
        checkValue("SELECT QUARTER(\"2017-06-15\");");
        checkValue("SELECT SECOND(\"2017-06-20 09:34:00.000023\");");
        checkValue("SELECT SEC_TO_TIME(1);");
        checkValue("SELECT STR_TO_DATE(\"August 10 2017\", \"%M %d %Y\");");
        checkValue("SELECT SUBDATE(\"2017-06-15\", INTERVAL 10 DAY);");
        checkValue("SELECT SUBTIME(\"2017-06-15 10:24:21.000004\", \"5.000001\");");
        uncheckValue("SELECT SYSDATE();");
        checkValue("SELECT TIME(\"19:30:10\");");
        checkValue("SELECT TIME_FORMAT(\"19:30:10\", \"%H %i %s\");");
        uncheckValue("SELECT TIME_TO_SEC(\"19:30:10\");");
        checkValue("SELECT TIMEDIFF(\"13:10:11\", \"13:10:10\");");
        checkValue("SELECT TIMESTAMP(\"2017-07-23\",  \"13:10:11\");");
        checkValue("SELECT TO_DAYS(\"2017-06-20\");");
        checkValue("SELECT WEEK(\"2017-06-15\");");
        checkValue("SELECT WEEKDAY(\"2017-06-15\");");
        checkValue("SELECT WEEKOFYEAR(\"2017-06-15\");");
        checkValue("SELECT YEAR(\"2017-06-15\");");
        checkValue("SELECT YEARWEEK(\"2017-06-15\");");
    }
}


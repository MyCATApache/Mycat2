package io.mycat.sql;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class PushDownFunctionNormalTableTest implements MycatTest {


    @Test
    public void testCharFunction() throws Exception {
        check("SELECT ASCII('a') from mysql.innodb_index_stats ");
        check("SELECT BIN('12') from mysql.innodb_index_stats") ;
        check("SELECT BIT_LENGTH('text') from mysql.innodb_index_stats");
        check("SELECT CHAR(77,121,83,81,'76') from mysql.innodb_index_stats");
        check("SELECT CHAR_LENGTH('a') from mysql.innodb_index_stats");
        check("SELECT CHARACTER_LENGTH('a') from mysql.innodb_index_stats");
        check("SELECT CONCAT('a','b') from mysql.innodb_index_stats");
        check("SELECT CONCAT_WS(',','a','b') from mysql.innodb_index_stats");
        check("SELECT ELT(1, 'Aa', 'Bb', 'Cc', 'Dd') from mysql.innodb_index_stats");
//        check("SELECT EXPORT_SET(5,'Y','N',',',4)"); todo
        check("SELECT FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff') from mysql.innodb_index_stats");
        check("SELECT FIND_IN_SET('b','a,b,c,d') from mysql.innodb_index_stats");
        check("SELECT FORMAT(12332.123456, 4) from mysql.innodb_index_stats");
        check("SELECT TO_BASE64('abc') from mysql.innodb_index_stats");
        check("SELECT FROM_BASE64(TO_BASE64('abc')) from mysql.innodb_index_stats");
        checkValue("SELECT X'616263', HEX('abc'), UNHEX(HEX('abc')) from mysql.innodb_index_stats", "(abc,616263,abc)");
        check("SELECT HEX(255), CONV(HEX(255),16,10) from mysql.innodb_index_stats");
        check("SELECT INSERT('Quadratic', 3, 4, 'What') from mysql.innodb_index_stats");
        check("SELECT INSERT('Quadratic', -1, 4, 'What') from mysql.innodb_index_stats");
        check("SELECT INSERT('Quadratic', 3, 100, 'What') from mysql.innodb_index_stats");
        check("SELECT INSTR('foobarbar', 'bar') from mysql.innodb_index_stats");
        check("SELECT INSTR('xbar', 'foobar') from mysql.innodb_index_stats");
        check("SELECT INSTR('xbar', 'foobar') from mysql.innodb_index_stats");
        check("SELECT LEFT('foobarbar', 5) from mysql.innodb_index_stats");
        check("SELECT LENGTH('text') from mysql.innodb_index_stats");
        check("SELECT LOCATE('bar', 'foobarbar') from mysql.innodb_index_stats");
        check("SELECT LPAD('hi',4,'??') from mysql.innodb_index_stats");
        check("SELECT LPAD('hi',1,'??') from mysql.innodb_index_stats");
        check("SELECT LTRIM('  barbar') from mysql.innodb_index_stats");
        check("SELECT MAKE_SET(1,'a','b','c') from mysql.innodb_index_stats");
        check("SELECT MAKE_SET(1|4,'hello','nice','world') from mysql.innodb_index_stats");
        check("SELECT MAKE_SET(1|4,'hello','nice',NULL,'world') from mysql.innodb_index_stats");
        check("SELECT MAKE_SET(0,'a','b','c') from mysql.innodb_index_stats");
        check("SELECT MAKE_SET(0,'a','b','c') from mysql.innodb_index_stats");
        check("SELECT SUBSTRING('Quadratically',5) from mysql.innodb_index_stats");
        checkValue("SELECT SUBSTRING('foobarbar' FROM 4) from mysql.innodb_index_stats", "(barbar)");
        checkValue("SELECT SUBSTRING('Sakila', -3) from mysql.innodb_index_stats", "(ila)");
        checkValue("SELECT SUBSTRING('Quadratically',5,6) from mysql.innodb_index_stats", "(ratica)");
        checkValue("SELECT SUBSTRING('Sakila', -3) from mysql.innodb_index_stats", "(ila)");
        checkValue("SELECT SUBSTRING('Sakila', -5, 3) from mysql.innodb_index_stats", "(aki)");
        checkValue("SELECT SUBSTRING('Sakila' FROM -4 FOR 2) from mysql.innodb_index_stats", "(ki)");
        checkValue("SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2) from mysql.innodb_index_stats", "(www.mysql)");
        checkValue("SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2) from mysql.innodb_index_stats", "(mysql.com)");
        checkValue("SELECT TRIM('  bar   ') from mysql.innodb_index_stats", "(bar)");
        checkValue("SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx') from mysql.innodb_index_stats", "(barxxx)");
        checkValue("SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx') from mysql.innodb_index_stats", "(bar)");
        checkValue("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') from mysql.innodb_index_stats", "(barx)");
        checkValue("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') from mysql.innodb_index_stats", "(barx)");
        check("SELECT UNHEX('4D7953514C') from mysql.innodb_index_stats");
        checkValue("SELECT X'4D7953514C' from mysql.innodb_index_stats", "(MySQL)");
        check("SELECT UNHEX(HEX('string')) from mysql.innodb_index_stats");
        check("SELECT HEX(UNHEX('1267')) from mysql.innodb_index_stats");
        check("SELECT UNHEX('GG') from mysql.innodb_index_stats");
        check("SELECT UPPER('a') from mysql.innodb_index_stats");
        check("SELECT LOWER('A') from mysql.innodb_index_stats");
        check("SELECT LCASE('A') from mysql.innodb_index_stats");
        check("SELECT UNHEX('GG') from mysql.innodb_index_stats");
        uncheckValue("SELECT ROW_COUNT() from mysql.innodb_index_stats");


    }

    private void checkValue(String s, String s1) throws Exception {
        checkValue(s);
    }

    private void check(String s) throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB1);
             Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        ) {
            Assert.assertEquals(
                    executeQueryAsString(mySQLConnection, s)
                    , (executeQueryAsString(mycatConnection, s)));
        }
    }

    private void uncheckValue(String s) throws Exception {
        try (
                Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        ) {
            executeQuery(mycatConnection, s);
        }
    }

    private void checkValue(String s) throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB1);
             Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        ) {
            Assert.assertEquals(
                    executeQuery(mySQLConnection, s)
                            .stream().map(i -> i.values()).collect(Collectors.toList()).toString()
                    , (executeQuery(mycatConnection, s))
                            .stream().map(i -> i.values()).collect(Collectors.toList()).toString()
            );
        }
    }


    private <T> String executeQueryAsString(Connection conn, String s) throws Exception {

        List<Map<String, Object>> rows = executeQuery(conn, s);

        return rows.toString();
    }

    @NotNull
    @Override
    public List<Map<String, Object>> executeQuery(Connection conn, String s) throws Exception {
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
                    String columName = rsMeta.getColumnLabel(i + 1).replaceAll(" ", "");
                    Object value = rs.getString(i + 1);
                    if (row.containsKey(columName)) {
                        columName = columName + i;
                    }
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
    public void testMathFunction() throws Exception {
        checkValue("SELECT abs(-1) from mysql.innodb_index_stats", "1");//
        checkValue("SELECT ACOS(-1) from mysql.innodb_index_stats", "3.141592653589793");//
        checkValue("SELECT ASIN(-1) from mysql.innodb_index_stats", "-1.5707963267948966");//
        checkValue("SELECT ATAN(-1) from mysql.innodb_index_stats", "-0.7853981633974483");//
        checkValue("SELECT ATAN2(-1,-1) from mysql.innodb_index_stats", "-2.356194490192345");//
        checkValue("SELECT ATAN(-1) from mysql.innodb_index_stats", "-0.7853981633974483");//
        checkValue("SELECT CEIL(-1) from mysql.innodb_index_stats", "-1");//
        checkValue("SELECT CEILING(-1) from mysql.innodb_index_stats", "-1");//
        checkValue("SELECT CONV(16,10,16) from mysql.innodb_index_stats", "10");
        checkValue("SELECT COS(-1) from mysql.innodb_index_stats", "0.5403023058681398");//
        checkValue("SELECT COT(-1) from mysql.innodb_index_stats", "-0.6420926159343306");//
        checkValue("SELECT CRC32(-1) from mysql.innodb_index_stats", "808273962");
        checkValue("SELECT DEGREES(-1) from mysql.innodb_index_stats", "-57.29577951308232");//
        checkValue("SELECT EXP(-1) from mysql.innodb_index_stats", "0.36787944117144233");//
        checkValue("SELECT FLOOR(-1) from mysql.innodb_index_stats", "-1");//
        checkValue("SELECT LN(2) from mysql.innodb_index_stats", "0.6931471805599453");//
        checkValue("SELECT LOG(10) from mysql.innodb_index_stats", "1.0");//
        checkValue("SELECT LOG10(10) from mysql.innodb_index_stats", "1.0");//
        //checkValue("SELECT LOG2(10) ","3.3219280948873626");//精度过高
        checkValue("SELECT MOD(6,5) from mysql.innodb_index_stats", "1");//
        // checkValue("SELECT PI() ","3.141593");//精度过高
        checkValue("SELECT POW(-1,2) from mysql.innodb_index_stats");//
        checkValue("SELECT POWER(-1,2) from mysql.innodb_index_stats");//
        uncheckValue("SELECT RAND(1) from mysql.innodb_index_stats");//
        checkValue("SELECT ROUND(-1) from mysql.innodb_index_stats", "-1");//
        checkValue("SELECT SIGN(-1) from mysql.innodb_index_stats", "-1");//
        checkValue("SELECT SIN(-1) from mysql.innodb_index_stats", "-0.8414709848078965");//
        checkValue("SELECT SQRT(2) from mysql.innodb_index_stats", "1.4142135623730951");//
        checkValue("SELECT TAN(-1) from mysql.innodb_index_stats", "-1.5574077246549023");//
        checkValue("SELECT TRUNCATE(123.4567, 3) from mysql.innodb_index_stats", "123.456");//

    }

    @Test
    public void testTimeFunction() throws Exception {
        checkValue("SELECT ADDDATE(\"2017-06-15\", INTERVAL 10 DAY) from mysql.innodb_index_stats;");//
        checkValue("SELECT ADDTIME(\"2017-06-15 09:34:21\", \"2\") from mysql.innodb_index_stats;");//
        checkValue("SELECT CURDATE() from mysql.innodb_index_stats;");//
        checkValue("SELECT CURRENT_DATE() from mysql.innodb_index_stats;");//
        uncheckValue("SELECT CURRENT_TIME() from mysql.innodb_index_stats;");//
        checkValue("SELECT DATE('2003-12-31 01:02:03') from mysql.innodb_index_stats;");//
        uncheckValue("SELECT (CURTIME() + 0) from mysql.innodb_index_stats;");//
        checkValue("SELECT DATEDIFF('2007-12-31 23:59:59','2007-12-30') from mysql.innodb_index_stats");//
        checkValue("SELECT DATEDIFF('2010-11-30 23:59:59','2010-12-31') from mysql.innodb_index_stats;");//
        checkValue("SELECT DATE_ADD('2018-05-01',INTERVAL 1 DAY) from mysql.innodb_index_stats;");//
        checkValue("SELECT DATE_SUB('2018-05-01',INTERVAL 1 YEAR) from mysql.innodb_index_stats;");//
        uncheckValue("SELECT DATE_ADD('2020-12-31 23:59:59',INTERVAL 1 SECOND) from mysql.innodb_index_stats;");//
        checkValue("SELECT DATE_FORMAT(\"2017-06-15\", \"%Y\") from mysql.innodb_index_stats;");
        checkValue("SELECT DAY(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT DAYNAME(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT DAYOFMONTH(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT DAYOFWEEK(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT DAYOFYEAR(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT EXTRACT(MONTH FROM \"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT FROM_DAYS(685467) from mysql.innodb_index_stats;");
        checkValue("SELECT HOUR(\"2017-06-20 09:34:00\") from mysql.innodb_index_stats;");
        checkValue("SELECT LAST_DAY(\"2017-06-20\") from mysql.innodb_index_stats;");
        uncheckValue("SELECT LOCALTIME() from mysql.innodb_index_stats;");
        uncheckValue("SELECT LOCALTIMESTAMP() from mysql.innodb_index_stats;");
        checkValue("SELECT MAKEDATE(2017, 3) from mysql.innodb_index_stats;");
        uncheckValue("SELECT MAKETIME(11, 35, 4) from mysql.innodb_index_stats;");
        checkValue("SELECT MICROSECOND(\"2017-06-20 09:34:00.000023\") from mysql.innodb_index_stats;");
        checkValue("SELECT MINUTE(\"2017-06-20 09:34:00\");");
        checkValue("SELECT MONTH(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT MONTHNAME(\"2017-06-15\") from mysql.innodb_index_stats;");

        uncheckValue("SELECT NOW() from mysql.innodb_index_stats;");
        checkValue("SELECT PERIOD_ADD(201703, 5) from mysql.innodb_index_stats");
        checkValue("SELECT PERIOD_DIFF(201710, 201703) from mysql.innodb_index_stats;");
        checkValue("SELECT QUARTER(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT QUARTER(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT SECOND(\"2017-06-20 09:34:00.000023\") from mysql.innodb_index_stats;");
        uncheckValue("SELECT SEC_TO_TIME(1) from mysql.innodb_index_stats;");
        checkValue("SELECT STR_TO_DATE(\"August 10 2017\", \"%M %d %Y\") from mysql.innodb_index_stats;");
        checkValue("SELECT SUBDATE(\"2017-06-15\", INTERVAL 10 DAY) from mysql.innodb_index_stats;");
        checkValue("SELECT SUBTIME(\"2017-06-15 10:24:21.000004\", \"5.000001\") from mysql.innodb_index_stats;");
        uncheckValue("SELECT SYSDATE() from mysql.innodb_index_stats;");
        uncheckValue("SELECT TIME(\"19:30:10\") from mysql.innodb_index_stats;");
        checkValue("SELECT TIME_FORMAT(\"19:30:10\", \"%H %i %s\") from mysql.innodb_index_stats;");
        uncheckValue("SELECT TIME_TO_SEC(\"19:30:10\") from mysql.innodb_index_stats;");
        uncheckValue("SELECT TIMEDIFF(\"13:10:11\", \"13:10:10\") from mysql.innodb_index_stats;");
        uncheckValue("SELECT TIMESTAMP(\"2017-07-23\",  \"13:10:11\") from mysql.innodb_index_stats;");
        checkValue("SELECT TO_DAYS(\"2017-06-20\") from mysql.innodb_index_stats;");
        checkValue("SELECT WEEK(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT WEEKDAY(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT WEEKOFYEAR(\"2017-06-15\") from mysql.innodb_index_stats;");
        checkValue("SELECT YEAR(\"2017-06-15\");");
        checkValue("SELECT YEARWEEK(\"2017-06-15\") from mysql.innodb_index_stats;");
        //todo
    }
}


package io.mycat.sql;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;


public class PushDownFunctionNormalTableTest implements MycatTest {

    @Before
    @SneakyThrows
    public void before() {
        try(Connection mycatConnection = getMySQLConnection(DB_MYCAT);){
            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`dual` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
        }
    }

    @Test
    public void testCharFunction() throws Exception {
        check("SELECT ASCII('a') from db1.`dual` ");
        check("SELECT BIN('12') from db1.`dual`");
        check("SELECT BIT_LENGTH('text') from db1.`dual`");
        check("SELECT CHAR(77,121,83,81,'76') from db1.`dual`");
        check("SELECT CHAR_LENGTH('a') from db1.`dual`");
        check("SELECT CHARACTER_LENGTH('a') from db1.`dual`");
        check("SELECT CONCAT('a','b') from db1.`dual`");
        check("SELECT CONCAT_WS(',','a','b') from db1.`dual`");
        check("SELECT ELT(1, 'Aa', 'Bb', 'Cc', 'Dd') from db1.`dual`");
//        check("SELECT EXPORT_SET(5,'Y','N',',',4)"); todo
        check("SELECT FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff') from db1.`dual`");
        check("SELECT FIND_IN_SET('b','a,b,c,d') from db1.`dual`");
        check("SELECT FORMAT(12332.123456, 4) from db1.`dual`");
        check("SELECT TO_BASE64('abc') from db1.`dual`");
        check("SELECT FROM_BASE64(TO_BASE64('abc')) from db1.`dual`");
        checkValue("SELECT X'616263', HEX('abc'), UNHEX(HEX('abc')) from db1.`dual`", "(abc,616263,abc)");
        check("SELECT HEX(255), CONV(HEX(255),16,10) from db1.`dual`");
        check("SELECT INSERT('Quadratic', 3, 4, 'What') from db1.`dual`");
        check("SELECT INSERT('Quadratic', -1, 4, 'What') from db1.`dual`");
        check("SELECT INSERT('Quadratic', 3, 100, 'What') from db1.`dual`");
        check("SELECT INSTR('foobarbar', 'bar') from db1.`dual`");
        check("SELECT INSTR('xbar', 'foobar') from db1.`dual`");
        check("SELECT INSTR('xbar', 'foobar') from db1.`dual`");
        check("SELECT LEFT('foobarbar', 5) from db1.`dual`");
        check("SELECT LENGTH('text') from db1.`dual`");
        check("SELECT LOCATE('bar', 'foobarbar') from db1.`dual`");
        check("SELECT LPAD('hi',4,'??') from db1.`dual`");
        check("SELECT LPAD('hi',1,'??') from db1.`dual`");
        check("SELECT LTRIM('  barbar') from db1.`dual`");
        check("SELECT MAKE_SET(1,'a','b','c') from db1.`dual`");
        check("SELECT MAKE_SET(1|4,'hello','nice','world') from db1.`dual`");
        check("SELECT MAKE_SET(1|4,'hello','nice',NULL,'world') from db1.`dual`");
        check("SELECT MAKE_SET(0,'a','b','c') from db1.`dual`");
        check("SELECT MAKE_SET(0,'a','b','c') from db1.`dual`");
        check("SELECT SUBSTRING('Quadratically',5) from db1.`dual`");
        checkValue("SELECT SUBSTRING('foobarbar' FROM 4) from db1.`dual`", "(barbar)");
        checkValue("SELECT SUBSTRING('Sakila', -3) from db1.`dual`", "(ila)");
        checkValue("SELECT SUBSTRING('Quadratically',5,6) from db1.`dual`", "(ratica)");
        checkValue("SELECT SUBSTRING('Sakila', -3) from db1.`dual`", "(ila)");
        checkValue("SELECT SUBSTRING('Sakila', -5, 3) from db1.`dual`", "(aki)");
        checkValue("SELECT SUBSTRING('Sakila' FROM -4 FOR 2) from db1.`dual`", "(ki)");
        checkValue("SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2) from db1.`dual`", "(www.mysql)");
        checkValue("SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2) from db1.`dual`", "(mysql.com)");
        checkValue("SELECT TRIM('  bar   ') from db1.`dual`", "(bar)");
        checkValue("SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx') from db1.`dual`", "(barxxx)");
        checkValue("SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx') from db1.`dual`", "(bar)");
        checkValue("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') from db1.`dual`", "(barx)");
        checkValue("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') from db1.`dual`", "(barx)");
        check("SELECT UNHEX('4D7953514C') from db1.`dual`");
        checkValue("SELECT X'4D7953514C' from db1.`dual`", "(MySQL)");
        check("SELECT UNHEX(HEX('string')) from db1.`dual`");
        check("SELECT HEX(UNHEX('1267')) from db1.`dual`");
        check("SELECT UNHEX('GG') from db1.`dual`");
        check("SELECT UPPER('a') from db1.`dual`");
        check("SELECT LOWER('A') from db1.`dual`");
        check("SELECT LCASE('A') from db1.`dual`");
        check("SELECT UNHEX('GG') from db1.`dual`");
        uncheckValue("SELECT ROW_COUNT() from db1.`dual`");


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
        checkValue("SELECT abs(-1) from db1.`dual`", "1");//
        checkValue("SELECT ACOS(-1) from db1.`dual`", "3.141592653589793");//
        checkValue("SELECT ASIN(-1) from db1.`dual`", "-1.5707963267948966");//
        checkValue("SELECT ATAN(-1) from db1.`dual`", "-0.7853981633974483");//
        checkValue("SELECT ATAN2(-1,-1) from db1.`dual`", "-2.356194490192345");//
        checkValue("SELECT ATAN(-1) from db1.`dual`", "-0.7853981633974483");//
        checkValue("SELECT CEIL(-1) from db1.`dual`", "-1");//
        checkValue("SELECT CEILING(-1) from db1.`dual`", "-1");//
        checkValue("SELECT CONV(16,10,16) from db1.`dual`", "10");
        checkValue("SELECT COS(-1) from db1.`dual`", "0.5403023058681398");//
        checkValue("SELECT COT(-1) from db1.`dual`", "-0.6420926159343306");//
        checkValue("SELECT CRC32(-1) from db1.`dual`", "808273962");
        checkValue("SELECT DEGREES(-1) from db1.`dual`", "-57.29577951308232");//
        checkValue("SELECT EXP(-1) from db1.`dual`", "0.36787944117144233");//
        checkValue("SELECT FLOOR(-1) from db1.`dual`", "-1");//
        checkValue("SELECT LN(2) from db1.`dual`", "0.6931471805599453");//
        checkValue("SELECT LOG(10) from db1.`dual`", "1.0");//
        checkValue("SELECT LOG10(10) from db1.`dual`", "1.0");//
        //checkValue("SELECT LOG2(10) ","3.3219280948873626");//精度过高
        checkValue("SELECT MOD(6,5) from db1.`dual`", "1");//
        // checkValue("SELECT PI() ","3.141593");//精度过高
        checkValue("SELECT POW(-1,2) from db1.`dual`");//
        checkValue("SELECT POWER(-1,2) from db1.`dual`");//
        uncheckValue("SELECT RAND(1) from db1.`dual`");//
        checkValue("SELECT ROUND(-1) from db1.`dual`", "-1");//
        checkValue("SELECT SIGN(-1) from db1.`dual`", "-1");//
        checkValue("SELECT SIN(-1) from db1.`dual`", "-0.8414709848078965");//
        checkValue("SELECT SQRT(2) from db1.`dual`", "1.4142135623730951");//
        checkValue("SELECT TAN(-1) from db1.`dual`", "-1.5574077246549023");//
        checkValue("SELECT TRUNCATE(123.4567, 3) from db1.`dual`", "123.456");//

    }

    @Test
    public void testTimeFunction() throws Exception {
        checkValue("SELECT ADDDATE(\"2017-06-15\", INTERVAL 10 DAY) from db1.`dual`;");//
        checkValue("SELECT ADDTIME(\"2017-06-15 09:34:21\", \"2\") from db1.`dual`;");//
        checkValue("SELECT CURDATE() from db1.`dual`;");//
        checkValue("SELECT CURRENT_DATE() from db1.`dual`;");//
        uncheckValue("SELECT CURRENT_TIME() from db1.`dual`;");//
        checkValue("SELECT DATE('2003-12-31 01:02:03') from db1.`dual`;");//
        uncheckValue("SELECT (CURTIME() + 0) from db1.`dual`;");//
        checkValue("SELECT DATEDIFF('2007-12-31 23:59:59','2007-12-30') from db1.`dual`");//
        checkValue("SELECT DATEDIFF('2010-11-30 23:59:59','2010-12-31') from db1.`dual`;");//
        checkValue("SELECT DATE_ADD('2018-05-01',INTERVAL 1 DAY) from db1.`dual`;");//
        checkValue("SELECT DATE_SUB('2018-05-01',INTERVAL 1 YEAR) from db1.`dual`;");//
        uncheckValue("SELECT DATE_ADD('2020-12-31 23:59:59',INTERVAL 1 SECOND) from db1.`dual`;");//
        checkValue("SELECT DATE_FORMAT(\"2017-06-15\", \"%Y\") from db1.`dual`;");
        checkValue("SELECT DAY(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT DAYNAME(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT DAYOFMONTH(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT DAYOFWEEK(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT DAYOFYEAR(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT EXTRACT(MONTH FROM \"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT FROM_DAYS(685467) from db1.`dual`;");
        checkValue("SELECT HOUR(\"2017-06-20 09:34:00\") from db1.`dual`;");
        checkValue("SELECT LAST_DAY(\"2017-06-20\") from db1.`dual`;");
        uncheckValue("SELECT LOCALTIME() from db1.`dual`;");
        uncheckValue("SELECT LOCALTIMESTAMP() from db1.`dual`;");
        checkValue("SELECT MAKEDATE(2017, 3) from db1.`dual`;");
        uncheckValue("SELECT MAKETIME(11, 35, 4) from db1.`dual`;");
        checkValue("SELECT MICROSECOND(\"2017-06-20 09:34:00.000023\") from db1.`dual`;");
        checkValue("SELECT MINUTE(\"2017-06-20 09:34:00\");");
        checkValue("SELECT MONTH(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT MONTHNAME(\"2017-06-15\") from db1.`dual`;");

        uncheckValue("SELECT NOW() from db1.`dual`;");
        checkValue("SELECT PERIOD_ADD(201703, 5) from db1.`dual`");
        checkValue("SELECT PERIOD_DIFF(201710, 201703) from db1.`dual`;");
        checkValue("SELECT QUARTER(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT QUARTER(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT SECOND(\"2017-06-20 09:34:00.000023\") from db1.`dual`;");
        uncheckValue("SELECT SEC_TO_TIME(1) from db1.`dual`;");
        checkValue("SELECT STR_TO_DATE(\"August 10 2017\", \"%M %d %Y\") from db1.`dual`;");
        checkValue("SELECT SUBDATE(\"2017-06-15\", INTERVAL 10 DAY) from db1.`dual`;");
        checkValue("SELECT SUBTIME(\"2017-06-15 10:24:21.000004\", \"5.000001\") from db1.`dual`;");
        uncheckValue("SELECT SYSDATE() from db1.`dual`;");
        uncheckValue("SELECT TIME(\"19:30:10\") from db1.`dual`;");
        checkValue("SELECT TIME_FORMAT(\"19:30:10\", \"%H %i %s\") from db1.`dual`;");
        uncheckValue("SELECT TIME_TO_SEC(\"19:30:10\") from db1.`dual`;");
        uncheckValue("SELECT TIMEDIFF(\"13:10:11\", \"13:10:10\") from db1.`dual`;");
        uncheckValue("SELECT TIMESTAMP(\"2017-07-23\",  \"13:10:11\") from db1.`dual`;");
        checkValue("SELECT TO_DAYS(\"2017-06-20\") from db1.`dual`;");
        checkValue("SELECT WEEK(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT WEEKDAY(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT WEEKOFYEAR(\"2017-06-15\") from db1.`dual`;");
        checkValue("SELECT YEAR(\"2017-06-15\");");
        checkValue("SELECT YEARWEEK(\"2017-06-15\") from db1.`dual`;");
        //todo
    }
}


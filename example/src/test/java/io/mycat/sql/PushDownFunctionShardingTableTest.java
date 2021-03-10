package io.mycat.sql;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class PushDownFunctionShardingTableTest implements MycatTest {

    @Before
    public void runBeforeTestMethod() throws Exception {
        initShardingTable();
    }

    private void initShardingTable() throws Exception {
        Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        execute(mycatConnection, RESET_CONFIG);
        Connection mysql3306 = getMySQLConnection(DB1);

        execute(mycatConnection, "DROP DATABASE db1");


        execute(mycatConnection, "CREATE DATABASE db1");

        execute(mycatConnection, CreateDataSourceHint
                .create("ds0",
                        DB1));
        execute(mycatConnection, CreateDataSourceHint
                .create("ds1",
                        DB2));

        execute(mycatConnection,
                CreateClusterHint.create("c0",
                        Arrays.asList("ds0"), Collections.emptyList()));
        execute(mycatConnection,
                CreateClusterHint.create("c1",
                        Arrays.asList("ds1"), Collections.emptyList()));

        execute(mycatConnection, "USE `db1`;");
        execute(mysql3306, "USE `db1`;");

        execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;");
        deleteData(mycatConnection, "db1", "travelrecord");
    }

    @Test
    public void testCharFunction() throws Exception {
        check("SELECT ASCII('a') from db1.`travelrecord` ");
        check("SELECT BIN('12') from db1.`travelrecord`");
        check("SELECT BIT_LENGTH('text') from db1.`travelrecord`");
        check("SELECT CHAR(77,121,83,81,'76') from db1.`travelrecord`");
        check("SELECT CHAR_LENGTH('a') from db1.`travelrecord`");
        check("SELECT CHARACTER_LENGTH('a') from db1.`travelrecord`");
        check("SELECT CONCAT('a','b') from db1.`travelrecord`");
        check("SELECT CONCAT_WS(',','a','b') from db1.`travelrecord`");
        check("SELECT ELT(1, 'Aa', 'Bb', 'Cc', 'Dd') from db1.`travelrecord`");
//        check("SELECT EXPORT_SET(5,'Y','N',',',4)"); todo
        check("SELECT FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff') from db1.`travelrecord`");
        check("SELECT FIND_IN_SET('b','a,b,c,d') from db1.`travelrecord`");
        check("SELECT FORMAT(12332.123456, 4) from db1.`travelrecord`");
        check("SELECT TO_BASE64('abc') from db1.`travelrecord`");
        check("SELECT FROM_BASE64(TO_BASE64('abc')) from db1.`travelrecord`");
        checkValue("SELECT X'616263', HEX('abc'), UNHEX(HEX('abc')) from db1.`travelrecord`", "(abc,616263,abc)");
        check("SELECT HEX(255), CONV(HEX(255),16,10) from db1.`travelrecord`");
        check("SELECT INSERT('Quadratic', 3, 4, 'What') from db1.`travelrecord`");
        check("SELECT INSERT('Quadratic', -1, 4, 'What') from db1.`travelrecord`");
        check("SELECT INSERT('Quadratic', 3, 100, 'What') from db1.`travelrecord`");
        check("SELECT INSTR('foobarbar', 'bar') from db1.`travelrecord`");
        check("SELECT INSTR('xbar', 'foobar') from db1.`travelrecord`");
        check("SELECT INSTR('xbar', 'foobar') from db1.`travelrecord`");
        check("SELECT LEFT('foobarbar', 5) from db1.`travelrecord`");
        check("SELECT LENGTH('text') from db1.`travelrecord`");
        check("SELECT LOCATE('bar', 'foobarbar') from db1.`travelrecord`");
        check("SELECT LPAD('hi',4,'??') from db1.`travelrecord`");
        check("SELECT LPAD('hi',1,'??') from db1.`travelrecord`");
        check("SELECT LTRIM('  barbar') from db1.`travelrecord`");
        check("SELECT MAKE_SET(1,'a','b','c') from db1.`travelrecord`");
        check("SELECT MAKE_SET(1|4,'hello','nice','world') from db1.`travelrecord`");
        check("SELECT MAKE_SET(1|4,'hello','nice',NULL,'world') from db1.`travelrecord`");
        check("SELECT MAKE_SET(0,'a','b','c') from db1.`travelrecord`");
        check("SELECT MAKE_SET(0,'a','b','c') from db1.`travelrecord`");
        check("SELECT SUBSTRING('Quadratically',5) from db1.`travelrecord`");
        checkValue("SELECT SUBSTRING('foobarbar' FROM 4) from db1.`travelrecord`", "(barbar)");
        checkValue("SELECT SUBSTRING('Sakila', -3) from db1.`travelrecord`", "(ila)");
        checkValue("SELECT SUBSTRING('Quadratically',5,6) from db1.`travelrecord`", "(ratica)");
        checkValue("SELECT SUBSTRING('Sakila', -3) from db1.`travelrecord`", "(ila)");
        checkValue("SELECT SUBSTRING('Sakila', -5, 3) from db1.`travelrecord`", "(aki)");
        checkValue("SELECT SUBSTRING('Sakila' FROM -4 FOR 2) from db1.`travelrecord`", "(ki)");
        checkValue("SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2) from db1.`travelrecord`", "(www.mysql)");
        checkValue("SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2) from db1.`travelrecord`", "(mysql.com)");
        checkValue("SELECT TRIM('  bar   ') from db1.`travelrecord`", "(bar)");
        checkValue("SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx') from db1.`travelrecord`", "(barxxx)");
        checkValue("SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx') from db1.`travelrecord`", "(bar)");
        checkValue("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') from db1.`travelrecord`", "(barx)");
        checkValue("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') from db1.`travelrecord`", "(barx)");
        check("SELECT UNHEX('4D7953514C') from db1.`travelrecord`");
        checkValue("SELECT X'4D7953514C' from db1.`travelrecord`", "(MySQL)");
        check("SELECT UNHEX(HEX('string')) from db1.`travelrecord`");
        check("SELECT HEX(UNHEX('1267')) from db1.`travelrecord`");
        check("SELECT UNHEX('GG') from db1.`travelrecord`");
        check("SELECT UPPER('a') from db1.`travelrecord`");
        check("SELECT LOWER('A') from db1.`travelrecord`");
        check("SELECT LCASE('A') from db1.`travelrecord`");
        check("SELECT UNHEX('GG') from db1.`travelrecord`");
        uncheckValue("SELECT ROW_COUNT() from db1.`travelrecord`");


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
        checkValue("SELECT abs(-1) from db1.`travelrecord`", "1");//
        checkValue("SELECT ACOS(-1) from db1.`travelrecord`", "3.141592653589793");//
        checkValue("SELECT ASIN(-1) from db1.`travelrecord`", "-1.5707963267948966");//
        checkValue("SELECT ATAN(-1) from db1.`travelrecord`", "-0.7853981633974483");//
        checkValue("SELECT ATAN2(-1,-1) from db1.`travelrecord`", "-2.356194490192345");//
        checkValue("SELECT ATAN(-1) from db1.`travelrecord`", "-0.7853981633974483");//
        checkValue("SELECT CEIL(-1) from db1.`travelrecord`", "-1");//
        checkValue("SELECT CEILING(-1) from db1.`travelrecord`", "-1");//
        checkValue("SELECT CONV(16,10,16) from db1.`travelrecord`", "10");
        checkValue("SELECT COS(-1) from db1.`travelrecord`", "0.5403023058681398");//
        checkValue("SELECT COT(-1) from db1.`travelrecord`", "-0.6420926159343306");//
        checkValue("SELECT CRC32(-1) from db1.`travelrecord`", "808273962");
        checkValue("SELECT DEGREES(-1) from db1.`travelrecord`", "-57.29577951308232");//
        checkValue("SELECT EXP(-1) from db1.`travelrecord`", "0.36787944117144233");//
        checkValue("SELECT FLOOR(-1) from db1.`travelrecord`", "-1");//
        checkValue("SELECT LN(2) from db1.`travelrecord`", "0.6931471805599453");//
        checkValue("SELECT LOG(10) from db1.`travelrecord`", "1.0");//
        checkValue("SELECT LOG10(10) from db1.`travelrecord`", "1.0");//
        //checkValue("SELECT LOG2(10) ","3.3219280948873626");//精度过高
        checkValue("SELECT MOD(6,5) from db1.`travelrecord`", "1");//
        // checkValue("SELECT PI() ","3.141593");//精度过高
        checkValue("SELECT POW(-1,2) from db1.`travelrecord`");//
        checkValue("SELECT POWER(-1,2) from db1.`travelrecord`");//
        uncheckValue("SELECT RAND(1) from db1.`travelrecord`");//
        checkValue("SELECT ROUND(-1) from db1.`travelrecord`", "-1");//
        checkValue("SELECT SIGN(-1) from db1.`travelrecord`", "-1");//
        checkValue("SELECT SIN(-1) from db1.`travelrecord`", "-0.8414709848078965");//
        checkValue("SELECT SQRT(2) from db1.`travelrecord`", "1.4142135623730951");//
        checkValue("SELECT TAN(-1) from db1.`travelrecord`", "-1.5574077246549023");//
        checkValue("SELECT TRUNCATE(123.4567, 3) from db1.`travelrecord`", "123.456");//

    }

    @Test
    public void testTimeFunction() throws Exception {
        checkValue("SELECT ADDDATE(\"2017-06-15\", INTERVAL 10 DAY) from db1.`travelrecord`;");//
        checkValue("SELECT ADDTIME(\"2017-06-15 09:34:21\", \"2\") from db1.`travelrecord`;");//
        checkValue("SELECT CURDATE() from db1.`travelrecord`;");//
        checkValue("SELECT CURRENT_DATE() from db1.`travelrecord`;");//
        uncheckValue("SELECT CURRENT_TIME() from db1.`travelrecord`;");//
        checkValue("SELECT DATE('2003-12-31 01:02:03') from db1.`travelrecord`;");//
        uncheckValue("SELECT (CURTIME() + 0) from db1.`travelrecord`;");//
        checkValue("SELECT DATEDIFF('2007-12-31 23:59:59','2007-12-30') from db1.`travelrecord`");//
        checkValue("SELECT DATEDIFF('2010-11-30 23:59:59','2010-12-31') from db1.`travelrecord`;");//
        checkValue("SELECT DATE_ADD('2018-05-01',INTERVAL 1 DAY) from db1.`travelrecord`;");//
        checkValue("SELECT DATE_SUB('2018-05-01',INTERVAL 1 YEAR) from db1.`travelrecord`;");//
        uncheckValue("SELECT DATE_ADD('2020-12-31 23:59:59',INTERVAL 1 SECOND) from db1.`travelrecord`;");//
        checkValue("SELECT DATE_FORMAT(\"2017-06-15\", \"%Y\") from db1.`travelrecord`;");
        checkValue("SELECT DAY(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT DAYNAME(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT DAYOFMONTH(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT DAYOFWEEK(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT DAYOFYEAR(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT EXTRACT(MONTH FROM \"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT FROM_DAYS(685467) from db1.`travelrecord`;");
        checkValue("SELECT HOUR(\"2017-06-20 09:34:00\") from db1.`travelrecord`;");
        checkValue("SELECT LAST_DAY(\"2017-06-20\") from db1.`travelrecord`;");
        uncheckValue("SELECT LOCALTIME() from db1.`travelrecord`;");
        uncheckValue("SELECT LOCALTIMESTAMP() from db1.`travelrecord`;");
        checkValue("SELECT MAKEDATE(2017, 3) from db1.`travelrecord`;");
        uncheckValue("SELECT MAKETIME(11, 35, 4) from db1.`travelrecord`;");
        checkValue("SELECT MICROSECOND(\"2017-06-20 09:34:00.000023\") from db1.`travelrecord`;");
        checkValue("SELECT MINUTE(\"2017-06-20 09:34:00\");");
        checkValue("SELECT MONTH(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT MONTHNAME(\"2017-06-15\") from db1.`travelrecord`;");

        uncheckValue("SELECT NOW() from db1.`travelrecord`;");
        checkValue("SELECT PERIOD_ADD(201703, 5) from db1.`travelrecord`");
        checkValue("SELECT PERIOD_DIFF(201710, 201703) from db1.`travelrecord`;");
        checkValue("SELECT QUARTER(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT QUARTER(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT SECOND(\"2017-06-20 09:34:00.000023\") from db1.`travelrecord`;");
        uncheckValue("SELECT SEC_TO_TIME(1) from db1.`travelrecord`;");
        checkValue("SELECT STR_TO_DATE(\"August 10 2017\", \"%M %d %Y\") from db1.`travelrecord`;");
        checkValue("SELECT SUBDATE(\"2017-06-15\", INTERVAL 10 DAY) from db1.`travelrecord`;");
        checkValue("SELECT SUBTIME(\"2017-06-15 10:24:21.000004\", \"5.000001\") from db1.`travelrecord`;");
        uncheckValue("SELECT SYSDATE() from db1.`travelrecord`;");
        uncheckValue("SELECT TIME(\"19:30:10\") from db1.`travelrecord`;");
        checkValue("SELECT TIME_FORMAT(\"19:30:10\", \"%H %i %s\") from db1.`travelrecord`;");
        uncheckValue("SELECT TIME_TO_SEC(\"19:30:10\") from db1.`travelrecord`;");
        uncheckValue("SELECT TIMEDIFF(\"13:10:11\", \"13:10:10\") from db1.`travelrecord`;");
        uncheckValue("SELECT TIMESTAMP(\"2017-07-23\",  \"13:10:11\") from db1.`travelrecord`;");
        checkValue("SELECT TO_DAYS(\"2017-06-20\") from db1.`travelrecord`;");
        checkValue("SELECT WEEK(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT WEEKDAY(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT WEEKOFYEAR(\"2017-06-15\") from db1.`travelrecord`;");
        checkValue("SELECT YEAR(\"2017-06-15\");");
        checkValue("SELECT YEARWEEK(\"2017-06-15\") from db1.`travelrecord`;");
        //todo
    }

    /**
     *  BINARY, CHAR, DATE, DATETIME, TIME,DECIMAL, SIGNED, UNSIGNED
     * @throws Exception
     */
    @Test
    public void testCastFunction() throws Exception {
        checkValue("SELECT CONVERT('abc' USING utf8);");
        checkValue("SELECT CONVERT('abc' USING utf8) from db1.`travelrecord`;");

        checkValue("SELECT CONVERT('abc' USING GBK)");
        checkValue("SELECT CONVERT('abc' USING GBK) from db1.`travelrecord`;");


        checkValue("SELECT CAST( 1231 AS BINARY ) AS result ");
        checkValue("SELECT CAST( 1231 AS BINARY ) AS result FROM db1.travelrecord;");

        checkValue("SELECT CAST('abc' AS BINARY)");
        checkValue("SELECT CAST('abc' AS BINARY) FROM db1.travelrecord;");

        checkValue("SELECT CAST('2018-1-1' AS DATE)");
        checkValue("SELECT CAST('2018-1-1' AS DATE) FROM db1.travelrecord;");

        checkValue("SELECT CAST('2018-1-1' AS DATETIME)");
        checkValue("SELECT CAST('2018-1-1' AS DATETIME) FROM db1.travelrecord;");

        checkValue("SELECT CAST('00:00:00' AS TIME)");
        checkValue("SELECT CAST('00:00:00' AS TIME) FROM db1.travelrecord;");

        checkValue("SELECT CAST('1' AS DECIMAL)");
        checkValue("SELECT CAST('1' AS DECIMAL) FROM db1.travelrecord;");

        checkValue("SELECT CAST('-1' AS SIGNED)");
        checkValue("SELECT CAST('-1' AS SIGNED) FROM db1.travelrecord;");

        checkValue("SELECT CAST('1' AS UNSIGNED)");
        checkValue("SELECT CAST('1' AS UNSIGNED) FROM db1.travelrecord;");
    }
}


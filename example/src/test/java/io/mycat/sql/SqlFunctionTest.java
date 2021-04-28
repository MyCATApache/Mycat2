package io.mycat.sql;

import com.alibaba.druid.util.JdbcUtils;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.BaselineAddHint;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class SqlFunctionTest implements MycatTest {


    @Test
    public void testCharFunction() throws Exception {
        check("SELECT ASCII('a')");
        check("SELECT BIN('12')");
        check("SELECT BIT_LENGTH('text')");
        check("SELECT CHAR(77,121,83,81,'76')");
        check("SELECT CHAR_LENGTH('a')");
        check("SELECT CHARACTER_LENGTH('a')");
        check("SELECT CONCAT('a','b')");
        check("SELECT CONCAT_WS(',','a','b')");
        check("SELECT ELT(1, 'Aa', 'Bb', 'Cc', 'Dd')");
//        check("SELECT EXPORT_SET(5,'Y','N',',',4)"); todo
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
        uncheckValue("SELECT ROW_COUNT() ");


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
            System.out.println(s);

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
        checkValue("SELECT abs(-1)", "1");//
        checkValue("SELECT ACOS(-1)", "3.141592653589793");//
        checkValue("SELECT ASIN(-1)", "-1.5707963267948966");//
        checkValue("SELECT ATAN(-1)", "-0.7853981633974483");//
        checkValue("SELECT ATAN2(-1,-1)", "-2.356194490192345");//
        checkValue("SELECT ATAN(-1)", "-0.7853981633974483");//
        checkValue("SELECT CEIL(-1)", "-1");//
        checkValue("SELECT CEILING(-1)", "-1");//
        checkValue("SELECT CONV(16,10,16) ", "10");
        checkValue("SELECT COS(-1)", "0.5403023058681398");//
        checkValue("SELECT COT(-1)", "-0.6420926159343306");//
        checkValue("SELECT CRC32(-1) ", "808273962");
        checkValue("SELECT DEGREES(-1)", "-57.29577951308232");//
        checkValue("SELECT EXP(-1)", "0.36787944117144233");//
        checkValue("SELECT FLOOR(-1)", "-1");//
        checkValue("SELECT LN(2)", "0.6931471805599453");//
        checkValue("SELECT LOG(10) ", "1.0");//
        checkValue("SELECT LOG10(10) ", "1.0");//
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
    public void testTimeFunction() throws Exception {
        checkValue("SELECT ADDDATE(\"2017-06-15\", INTERVAL 10 DAY);");//
        checkValue("SELECT ADDTIME(\"2017-06-15 09:34:21\", \"2\");");//
        checkValue("SELECT CURDATE();");//
        checkValue("SELECT CURRENT_DATE();");//
        uncheckValue("SELECT CURRENT_TIME();");//
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
        uncheckValue("SELECT LOCALTIME();");
        uncheckValue("SELECT LOCALTIMESTAMP();");
        checkValue("SELECT MAKEDATE(2017, 3);");
        checkValue("SELECT MAKETIME(11, 35, 4);");
        checkValue("SELECT MICROSECOND(\"2017-06-20 09:34:00.000023\");");
        checkValue("SELECT MINUTE(\"2017-06-20 09:34:00\");");
        checkValue("SELECT MONTH(\"2017-06-15\");");
        checkValue("SELECT MONTHNAME(\"2017-06-15\");");

        uncheckValue("SELECT NOW();");
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
        //todo
    }

    @Test
    public void testGUIFunction() throws Exception {
        try(Connection mySQLConnection = getMySQLConnection(DB_MYCAT)){
            JdbcUtils.executeQuery(mySQLConnection,"SHOW STATUS",Collections.emptyList());
            JdbcUtils.execute(mySQLConnection,"FLUSH TABLES");
            JdbcUtils.execute(mySQLConnection,"FLUSH PRIVILEGES");
        }
    }

    @Test
    public void testAggFunction() throws Exception {
        initShardingTable();

        checkValue("select id from db1.travelrecord GROUP BY id  order by id ", "(1)(999999999)");
        checkValue("select id from db1.travelrecord GROUP BY id,user_id order by id ", "(999999999)(1)");
        checkValue("select id from db1.travelrecord GROUP BY id,user_id having id != 1 order by id", "(999999999)");
        checkValue("select id from db1.travelrecord GROUP BY id,user_id having max(id) > 1 order by id", "(999999999)");
        checkValue("select id,COUNT(user_id) from db1.travelrecord GROUP BY id order by id", "(1,1)(999999999,1)");
        checkValue("select id,COUNT(DISTINCT user_id) from db1.travelrecord GROUP BY id order by id", "(1,1)(999999999,1)");
        checkValue("select MAX(id) from db1.travelrecord order by any_value(id)", "(999999999)");
        checkValue("select MIN(id) from db1.travelrecord order by any_value(id)", "(1)");
        checkValue("select id,sum(id) from db1.travelrecord GROUP BY id order by id limit 2", "(1,1)(999999999,999999999)");
        uncheckValue("select id,avg(id*1.1) from db1.travelrecord GROUP BY id order by id limit 2");

        checkValue("select id from db1.travelrecord order by id asc", "(1)(999999999)");
        checkValue("select id from db1.travelrecord order by id desc", "(999999999)(1)");

        checkValue("select id from db1.travelrecord order by id desc limit 1", "(999999999)");
        checkValue("select id from db1.travelrecord order by id desc limit 1,1", "(1)");
        checkValue("select id from db1.travelrecord order by id desc limit 1 offset 0", "(999999999)");
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
        execute(mycatConnection, "CREATE TABLE `company` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))");
        execute(mysql3306, "CREATE TABLE if not exists db1.`travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT\n," +
                "  `user_id` varchar(100) DEFAULT NULL" +
                " , PRIMARY KEY (`id`) " +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
        execute(mysql3306, "CREATE TABLE if not exists `company` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))");

        execute(mycatConnection, "delete from db1.travelrecord");
        execute(mysql3306, "delete from db1.travelrecord");
        execute(mycatConnection, "delete from `db1`.`company`");
        execute(mysql3306, "delete from `db1`.`company`");

        for (int i = 1; i < 10; i++) {
            execute(mycatConnection, "insert db1.travelrecord (id) values(" + i + ")");
            execute(mysql3306, "insert db1.travelrecord (id) values(" + i + ")");
        }

        execute(mycatConnection, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (1,'Intel',1)");
        execute(mycatConnection, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (2,'IBM',2)");
        execute(mycatConnection, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (3,'Dell',3)");

//
//        execute(mysql3306, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (1,'Intel',1)");
//        execute(mysql3306, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (2,'IBM',2)");
//        execute(mysql3306, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (3,'Dell',3)");
//

        mycatConnection.close();
        mysql3306.close();
    }


    @Test
    public void testComplexQuery() throws Exception {
        initShardingTable();

        checkValue("select t.* from db1.travelrecord t order by t.id");

        checkValue("SELECT * FROM `travelrecord` WHERE (ISNULL(`id`)) AND (`user_id`='3') AND (`traveldate`='2020-12-25') AND (`fee`='333') AND (`days`='111') AND (`blob`='张三') LIMIT 1", "");
        checkValue("select * from db1.travelrecord as t,db1.company as c  where t.id = c.id order by  t.id", "(1,999,null,null,null,null,1,Intel,1)");
        checkValue("select * from db1.travelrecord as t INNER JOIN db1.company as c  on  t.id = c.id order by  t.id", "(1,999,null,null,null,null,1,Intel,1)");
        checkValue("select * from db1.travelrecord as t LEFT  JOIN db1.company as c  on  t.id = c.id order by  t.id", "(999999999,999,null,null,null,null,null,null,null)(1,999,null,null,null,null,1,Intel,1)");
        checkValue("select * from db1.travelrecord as t RIGHT   JOIN db1.company as c  on  t.id = c.id order by  t.id", "(1,999,null,null,null,null,1,Intel,1)(null,null,null,null,null,null,2,IBM,2)(null,null,null,null,null,null,3,Dell,3)");

        //三表
        checkValue(
                "select * from (db1.travelrecord as t LEFT  JOIN db1.company as c  on  t.id = c.id )  LEFT  JOIN db1.company as c2 on t.id = c2.id order by t.id");


        // checkValue("select (select c.id from db1.company as c  where c.id = t.id) from db1.travelrecord as t where t.id = 1 order by t.id", "(1)"); todo
        checkValue("select * from db1.travelrecord as t where  EXISTS (select id from db1.company as c where t.id =c.id ) order by t.id", "(1,999,null,null,null,null)");
        checkValue("select * from db1.travelrecord as t where not EXISTS (select id from db1.company as c where t.id =c.id ) order by t.id", "(999999999,999,null,null,null,null)");

        checkValue("select * from db1.travelrecord t where id in (select id from db1.company) order by t.id;", "(1,999,null,null,null,null)");
        checkValue("select * from db1.travelrecord t where id in (1,999) order by t.id;", "(1,999,null,null,null,null)");


        int min = 1;
        int max = 999;
        /*
        SELECT [ALL | DISTINCT]
                */
        checkValue("select distinct(user_id) from db1.travelrecord t ", "999");
        checkValue("select all(user_id) from db1.travelrecord t ", "(999)(999)");

        /*
        SELECT [ALL | DISTINCT] select_expr [, select_expr ...]  FROM table_references
         */
        //     算术表达式测试
        checkValue("select user_id+id from db1.travelrecord", 999 + max + ")(" + (999 + min));
        checkValue("select user_id-id from db1.travelrecord", (999 - max) + ")(" + (999 - min));
        checkValue("select user_id*id from db1.travelrecord", (999 * max) + ")(" + (999 * min));
        checkValue("select (user_id*1.0)/(id*1.0) from db1.travelrecord");//结果不确定
//        check("select user_id DIV id from db1.travelrecord",(999/max)+")("+(999/min));不支持
        checkValue("select user_id % id from db1.travelrecord");//结果不确定
        checkValue("select user_id MOD id from db1.travelrecord");//结果不确定


        /*
        SELECT [ALL | DISTINCT] select_expr [, select_expr ...] [FROM table_references  [WHERE where_condition]
        逻辑运算符 测试
         */
        checkValue("select id,user_id from db1.travelrecord where id = " + min, "(" + min + ",999)");
        checkValue("select id,user_id from db1.travelrecord where id = " + max, "(" + max + ",999)");

        //or表达式
        checkValue("select id,user_id from db1.travelrecord where id = " + min + " or " + " id = " + max + " order by id", "(" + min + ",999)" + "(" + max + ",999)");

        //and表达式
        checkValue("select id,user_id from db1.travelrecord where id = " + min + " and " + " user_id = 999 order by id", "(" + min + ",999)");

        //not表达式
        checkValue("select id,user_id from db1.travelrecord where !(id = " + min + " and " + " user_id = 999" + ") order by id", "(" + max + ",999)");

        //between
        checkValue("select id,user_id from db1.travelrecord where id between 1 and 999 order by id", "(" + min + ",999)");

        //like
        checkValue("select id,user_id from db1.travelrecord where user_id LIKE '99%' order by id");

        checkValue(" SELECT *,\n" +
                "   rank() over (PARTITION BY id\n" +
                "                 ORDER BY user_id DESC) AS ranking\n" +
                "FROM db1.`travelrecord`");

        checkValue("select 1");

        checkValue("SELECT\n" +
                "    user_id,\n" +
                "    SUM(id) over (PARTITION BY id) sum_user_id\n" +
                "FROM db1.travelrecord ORDER BY sum_user_id;");

//        checkValue("select\n" +
//                "    rank() over (partition by user_id order by id) as rank\n" +
//                "from db1.travelrecord;");
    }


    @Test
    public void testInsertFunction() throws Exception {
        Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        execute(mycatConnection, RESET_CONFIG);
        Connection mysql3306 = getMySQLConnection(DB1);

        execute(mycatConnection, "DROP DATABASE db1");


        execute(mycatConnection, "CREATE DATABASE db1");
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

        execute(mycatConnection, "CREATE TABLE `travelrecord2` (\n" +
                "  `id` bigint(20) NOT NULL KEY,\n" +
                "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                "  `traveldate` datetime DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int(11) DEFAULT NULL,\n" +
                "  `blob` longblob DEFAULT NULL\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "tbpartition by YYYYMM(traveldate) tbpartitions 12;");
        deleteData(mycatConnection, "db1", "travelrecord2");
        execute(mycatConnection, "INSERT INTO `travelrecord2`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`)\n" +
                "VALUES (1,2,timestamp('2021-02-22 18:34:05.983692'),3,4,NULL)");

    }

    @Test
    public void testOptimizationProcedure() throws Exception {
        initShardingTable();
        try(Connection mySQLConnection = getMySQLConnection(DB_MYCAT);){
            List<Map<String, Object>> step0 = JdbcUtils.executeQuery(mySQLConnection,
                    BaselineAddHint.create("select * from db1.travelrecord n join db1.company s on n.id = s.id and n.id = 1"),
                    Collections.emptyList());

            System.out.println(step0);

            List<Map<String, Object>> explainStep0 = JdbcUtils.executeQuery(mySQLConnection,
                ("explain select * from db1.travelrecord n join db1.company s on n.id = s.id and n.id = 1"),
                    Collections.emptyList());

            System.out.println(explainStep0);

            List<Map<String, Object>> step1 = JdbcUtils.executeQuery(mySQLConnection,
                    BaselineAddHint.create(true,
                            "/*+MYCAT:use_nl_join(n,s) */select * from db1.travelrecord n join db1.company s on n.id = s.id and n.id = 1"),
                    Collections.emptyList());

            System.out.println(step1);

            List<Map<String, Object>> explainStep1 = JdbcUtils.executeQuery(mySQLConnection,
               ("explain select * from db1.travelrecord n join db1.company s on n.id = s.id and n.id = 1"),
                    Collections.emptyList());

            System.out.println(explainStep1);



            List<Map<String, Object>> step2 = JdbcUtils.executeQuery(mySQLConnection,
                    BaselineAddHint.create(true,
                            "/*+MYCAT:use_hash_join(n,s) */select * from db1.travelrecord n join db1.company s on n.id = s.id and n.id = 1"),
                    Collections.emptyList());

            System.out.println(step2);

            List<Map<String, Object>> explainStep2 = JdbcUtils.executeQuery(mySQLConnection,
                    ("explain select * from db1.travelrecord n join db1.company s on n.id = s.id and n.id = 1"),
                    Collections.emptyList());

            System.out.println(explainStep2);

            List<Map<String, Object>> step3 = JdbcUtils.executeQuery(mySQLConnection,
                    BaselineAddHint.create(true,
                            "/*+MYCAT:use_merge_join(n,s) */select * from db1.travelrecord n join db1.company s on n.id = s.id and n.id = 1"),
                    Collections.emptyList());

            System.out.println(step3);

            List<Map<String, Object>> explainStep3 = JdbcUtils.executeQuery(mySQLConnection,
                    ("explain select * from db1.travelrecord n join db1.company s on n.id = s.id and n.id = 1"),
                    Collections.emptyList());

            System.out.println(explainStep3);


            Assert.assertTrue(explainStep1.toString().contains("NestedLoopJoin"));
            Assert.assertTrue(explainStep2.toString().contains("MycatHashJoin"));
            Assert.assertTrue(explainStep3.toString().contains("MycatSortMergeJoin"));


            System.out.println();
        }
    }
}


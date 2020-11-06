package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class CharChecker extends BaseChecker {
    public CharChecker(Statement statement) {
        super(statement);
    }

    @Override
    public void run() {
        check("delete from db1.travelrecord");
        executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (1,999)");

        simplyCheck("ASCII('a')", "(97)");
        simplyCheck("BIN('12')", "1100");
        simplyCheck("BIT_LENGTH('text')", "32");
        simplyCheck("CHAR(77,121,83,81,'76')", "MySQL");
        simplyCheck("CHAR_LENGTH('a')", "1");
        simplyCheck("CHARACTER_LENGTH('a')", "1");
        simplyCheck("CONCAT('a','b')", "ab");
        simplyCheck("CONCAT_WS(',','a','b')", "a,b");
        simplyCheck("ELT(1, 'Aa', 'Bb', 'Cc', 'Dd')", "(Aa)");
        simplyCheck("EXPORT_SET(5,'Y','N',',',4)", "(Y,N,Y,N)");
        simplyCheck("FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff')", "(2)");
        simplyCheck("FIND_IN_SET('b','a,b,c,d')", "(2)");
        simplyCheck("FORMAT(12332.123456, 4)", "(12,332.1235)");
        simplyCheck("TO_BASE64('abc')", "(YWJj)");
        simplyCheck("FROM_BASE64(TO_BASE64('abc'))", "(abc)");
        check("SELECT X'616263', HEX('abc'), UNHEX(HEX('abc')) from db1.travelrecord where id = 1 limit 1", "(abc,616263,abc)");
        check("SELECT HEX(255), CONV(HEX(255),16,10) from db1.travelrecord where id = 1 limit 1", "(FF,255)");
        check("SELECT INSERT('Quadratic', 3, 4, 'What') from db1.travelrecord where id = 1 limit 1", "(QuWhattic)");
        check("SELECT INSERT('Quadratic', -1, 4, 'What') from db1.travelrecord where id = 1 limit 1", "(Quadratic)");
        check("SELECT INSERT('Quadratic', 3, 100, 'What') from db1.travelrecord where id = 1 limit 1", "(QuWhat)");
        check("SELECT INSTR('foobarbar', 'bar') from db1.travelrecord where id = 1 limit 1", "(4)");
        check("SELECT INSTR('xbar', 'foobar') from db1.travelrecord where id = 1 limit 1", "(0)");
        check("SELECT INSTR('xbar', 'foobar') from db1.travelrecord where id = 1 limit 1", "(0)");
        check("SELECT LEFT('foobarbar', 5) from db1.travelrecord where id = 1 limit 1", "(fooba)");
        check("SELECT LENGTH('text') from db1.travelrecord where id = 1 limit 1", "(4)");
        check("SELECT LOCATE('bar', 'foobarbar') from db1.travelrecord where id = 1 limit 1", "(4)");
        check("SELECT LPAD('hi',4,'??') from db1.travelrecord where id = 1 limit 1", "(??hi)");
        check("SELECT LPAD('hi',1,'??') from db1.travelrecord where id = 1 limit 1", "(h)");
        check("SELECT LTRIM('  barbar') from db1.travelrecord where id = 1 limit 1", "(barbar)");
        check("SELECT MAKE_SET(1,'a','b','c') from db1.travelrecord where id = 1 limit 1", "(a)");
        check("SELECT MAKE_SET(1|4,'hello','nice','world') from db1.travelrecord where id = 1 limit 1", "(hello,world)");
        check("SELECT MAKE_SET(1|4,'hello','nice',NULL,'world') from db1.travelrecord where id = 1 limit 1", "(hello)");
        check("SELECT MAKE_SET(0,'a','b','c') from db1.travelrecord where id = 1 limit 1", "()");
        check("SELECT MAKE_SET(0,'a','b','c') from db1.travelrecord where id = 1 limit 1", "()");
        check("SELECT SUBSTRING('Quadratically',5) from db1.travelrecord where id = 1 limit 1", "(ratically)");
        check("SELECT SUBSTRING('foobarbar' FROM 4) from db1.travelrecord where id = 1 limit 1", "(barbar)");
        check("SELECT SUBSTRING('Sakila', -3) from db1.travelrecord where id = 1 limit 1", "(ila)");
        check("SELECT SUBSTRING('Quadratically',5,6) from db1.travelrecord where id = 1 limit 1", "(ratica)");
        check("SELECT SUBSTRING('Sakila', -3) from db1.travelrecord where id = 1 limit 1", "(ila)");
        check("SELECT SUBSTRING('Sakila', -5, 3) from db1.travelrecord where id = 1 limit 1", "(aki)");
        check("SELECT SUBSTRING('Sakila' FROM -4 FOR 2) from db1.travelrecord where id = 1 limit 1", "(ki)");
        check("SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2) from db1.travelrecord where id = 1 limit 1", "(www.mysql)");
        check("SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2) from db1.travelrecord where id = 1 limit 1", "(mysql.com)");
        check("SELECT TRIM('  bar   ') from db1.travelrecord where id = 1 limit 1", "(bar)");
        check("SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx') from db1.travelrecord where id = 1 limit 1", "(barxxx)");
        check("SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx') from db1.travelrecord where id = 1 limit 1", "(bar)");
        check("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') from db1.travelrecord where id = 1 limit 1", "(barx)");
        check("SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz') from db1.travelrecord where id = 1 limit 1", "(barx)");
        check("SELECT UNHEX('4D7953514C') from db1.travelrecord where id = 1 limit 1", "(MySQL)");
        check("SELECT X'4D7953514C' from db1.travelrecord where id = 1 limit 1", "(MySQL)");
        check("SELECT UNHEX(HEX('string')) from db1.travelrecord where id = 1 limit 1", "(string)");
        check("SELECT HEX(UNHEX('1267')) from db1.travelrecord where id = 1 limit 1", "(1267)");
        check("SELECT UNHEX('GG') from db1.travelrecord where id = 1 limit 1", "(null)");
        check("SELECT UPPER('a') from db1.travelrecord where id = 1 limit 1", "(A)");
        check("SELECT LOWER('A') from db1.travelrecord where id = 1 limit 1", "(a)");
        check("SELECT LCASE('A') from db1.travelrecord where id = 1 limit 1", "(a)");
        check("SELECT UNHEX('GG') from db1.travelrecord where id = 1 limit 1", "(null)");
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
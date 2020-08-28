package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class MathChecker extends BaseChecker {

    public MathChecker(Statement statement) {
        super(statement);
    }

    public void run() {
        check("delete from db1.travelrecord");
        executeUpdate("INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (1,999)");

        check("select 1 from db1.travelrecord where id = 1 limit 1");//

        simplyCheck("abs(-1)", "1");//
        simplyCheck("ACOS(-1)", "3.141592653589793");//
        simplyCheck("ASIN(-1)", "-1.5707963267948966");//
        simplyCheck("ATAN(-1)", "-0.7853981633974483");//
        simplyCheck("ATAN2(-1,-1)", "-2.356194490192345");//
        simplyCheck("ATAN(-1)", "-0.7853981633974483");//
        simplyCheck("CEIL(-1)", "-1");//
        simplyCheck("CEILING(-1)", "-1");//
//        check("select CONV(16,10,16) from db1.travelrecord where id = 1 limit 1","1");//不支持
        simplyCheck("COS(-1)", "0.5403023058681398");//
        simplyCheck("COT(-1)", "-0.6420926159343306");//
//        check("select CRC32(-1) from db1.travelrecord where id = 1 limit 1","1");//不支持
        simplyCheck("DEGREES(-1)", "-57.29577951308232");//
        simplyCheck("EXP(-1)", "0.36787944117144233");//
        simplyCheck("FLOOR(-1)", "-1");//
        simplyCheck("LN(2)", "0.6931471805599453");//
//        check("select LOG(10) from db1.travelrecord where id = 1 limit 1","1");//不支持
//        check("select LOG10(-1) from db1.travelrecord where id = 1 limit 1","1");//不支持
//        check("select LOG2(-1) from db1.travelrecord where id = 1 limit 1","1");//不支持
        simplyCheck("MOD(6,5)", "1");//
        check("select PI() from db1.travelrecord where id = 1 limit 1","3.141593");
        simplyCheck("POW(-1,2)");//
        simplyCheck("POWER(-1,2)");//
        simplyCheck("RAND(-1)");//
        simplyCheck("ROUND(-1)", "-1");//
        simplyCheck("SIGN(-1)", "-1");//
        simplyCheck("SIN(-1)", "-0.8414709848078965");//
        simplyCheck("SQRT(2)", "1.4142135623730951");//
        simplyCheck("TAN(-1)", "-1.5574077246549023");//
        simplyCheck("TRUNCATE(123.4567, 3)", "123.456");//
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

}
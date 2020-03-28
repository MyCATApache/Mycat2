package io.mycat.sql;

import java.sql.Statement;
import java.time.LocalDate;

public class MathChecker extends BaseChecker {

    public MathChecker(Statement statement) {
        super(statement);
    }

    public void run() {
        check("select 1 from db1.travelrecord where id = 1 limit 1", "(1)");//
        check("select CURDATE() from db1.travelrecord where id = 1 limit 1", LocalDate.now().toString());//
        check("select curdate() from db1.travelrecord where id = 1 limit 1", LocalDate.now().toString());//
        check("select now() from db1.travelrecord where id = 1 limit 1");//
        check("select abs(-1) from db1.travelrecord where id = 1 limit 1","1");//
        check("select ACOS(-1) from db1.travelrecord where id = 1 limit 1","3.141592653589793");//
        check("select ASIN(-1) from db1.travelrecord where id = 1 limit 1","-1.5707963267948966");//
        check("select ATAN(-1) from db1.travelrecord where id = 1 limit 1","-0.7853981633974483");//
        check("select ATAN2(-1,-1) from db1.travelrecord where id = 1 limit 1","-2.356194490192345");//
        check("select ATAN(-1) from db1.travelrecord where id = 1 limit 1","-0.7853981633974483");//
        check("select CEIL(-1) from db1.travelrecord where id = 1 limit 1","-1");//
        check("select CEILING(-1) from db1.travelrecord where id = 1 limit 1","-1");//
//        check("select CONV(16,10,16) from db1.travelrecord where id = 1 limit 1","1");//不支持
        check("select COS(-1) from db1.travelrecord where id = 1 limit 1","0.5403023058681398");//
        check("select COT(-1) from db1.travelrecord where id = 1 limit 1","-0.6420926159343306");//
//        check("select CRC32(-1) from db1.travelrecord where id = 1 limit 1","1");//不支持
        check("select DEGREES(-1) from db1.travelrecord where id = 1 limit 1","-57.29577951308232");//
        check("select EXP(-1) from db1.travelrecord where id = 1 limit 1","0.36787944117144233");//
        check("select FLOOR(-1) from db1.travelrecord where id = 1 limit 1","-1");//
        check("select LN(2) from db1.travelrecord where id = 1 limit 1","0.6931471805599453");//
//        check("select LOG(10) from db1.travelrecord where id = 1 limit 1","1");//不支持
//        check("select LOG10(-1) from db1.travelrecord where id = 1 limit 1","1");//不支持
//        check("select LOG2(-1) from db1.travelrecord where id = 1 limit 1","1");//不支持
        check("select MOD(6,5) from db1.travelrecord where id = 1 limit 1","1");//
//        check("select PI() from db1.travelrecord where id = 1 limit 1","1");//不支持
        check("select POW(-1,2) from db1.travelrecord where id = 1 limit 1");//
        check("select POWER(-1,2) from db1.travelrecord where id = 1 limit 1");//
        check("select RAND(-1) from db1.travelrecord where id = 1 limit 1");//
        check("select ROUND(-1) from db1.travelrecord where id = 1 limit 1","-1");//
        check("select SIGN(-1) from db1.travelrecord where id = 1 limit 1","-1");//
        check("select SIN(-1) from db1.travelrecord where id = 1 limit 1","-0.8414709848078965");//
        check("select SQRT(2) from db1.travelrecord where id = 1 limit 1","1.4142135623730951");//
        check("select TAN(-1) from db1.travelrecord where id = 1 limit 1","-1.5574077246549023");//
        check("select TRUNCATE(123.4567, 3) from db1.travelrecord where id = 1 limit 1","123.456");//
    }

}
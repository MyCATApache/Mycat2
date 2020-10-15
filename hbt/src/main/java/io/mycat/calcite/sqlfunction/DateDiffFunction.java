package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.time.LocalTime;
import java.util.Objects;

/**
 * DATEDIFF
 * Syntax
 * DATEDIFF(expr1,expr2)
 * Description
 * DATEDIFF() returns (expr1 – expr2) expressed as a value in days from one date to the other. expr1 and expr2 are date or date-and-time expressions. Only the date parts of the values are used in the calculation.
 *
 * Examples
 * SELECT DATEDIFF('2007-12-31 23:59:59','2007-12-30');
 * +----------------------------------------------+
 * | DATEDIFF('2007-12-31 23:59:59','2007-12-30') |
 * +----------------------------------------------+
 * |                                            1 |
 * +----------------------------------------------+
 *
 * SELECT DATEDIFF('2010-11-30 23:59:59','2010-12-31');
 * +----------------------------------------------+
 * | DATEDIFF('2010-11-30 23:59:59','2010-12-31') |
 * +----------------------------------------------+
 * |                                          -31 |
 * +----------------------------------------------+
 * CREATE TABLE t1 (d DATETIME);
 * INSERT INTO t1 VALUES
 *     ("2007-01-30 21:31:07"),
 *     ("1983-10-15 06:42:51"),
 *     ("2011-04-21 12:34:56"),
 *     ("2011-10-30 06:31:41"),
 *     ("2011-01-30 14:03:25"),
 *     ("2004-10-07 11:19:34");
 * SELECT NOW();
 * +---------------------+
 * | NOW()               |
 * +---------------------+
 * | 2011-05-23 10:56:05 |
 * +---------------------+
 *
 * SELECT d, DATEDIFF(NOW(),d) FROM t1;
 * +---------------------+-------------------+
 * | d                   | DATEDIFF(NOW(),d) |
 * +---------------------+-------------------+
 * | 2007-01-30 21:31:07 |              1574 |
 * | 1983-10-15 06:42:51 |             10082 |
 * | 2011-04-21 12:34:56 |                32 |
 * | 2011-10-30 06:31:41 |              -160 |
 * | 2011-01-30 14:03:25 |               113 |
 * | 2004-10-07 11:19:34 |              2419 |
 * +---------------------+-------------------+
 */
public class DateDiffFunction {
    public static String eval(String arg0,String arg1) {
        return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("DATEDIFF",arg0,arg1)));
    }

}
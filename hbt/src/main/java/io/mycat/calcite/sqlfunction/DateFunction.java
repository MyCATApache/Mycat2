package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.time.LocalTime;
import java.util.Objects;

/**
 * DATE FUNCTION
 * Syntax
 * DATE(expr)
 * Description
 * Extracts the date part of the date or datetime expression expr.
 *
 * Examples
 * SELECT DATE('2013-07-18 12:21:32');
 * +-----------------------------+
 * | DATE('2013-07-18 12:21:32') |
 * +-----------------------------+
 * | 2013-07-18                  |
 * +-----------------------------+
 * Error Handling
 * Until MariaDB 5.5.32, some versions of MariaDB returned 0000-00-00 when passed an invalid date. From 5.5.32, NULL is returned.
 */
public class DateFunction {
    public static String eval(String arg) {
        return Objects.toString(UnsolvedMysqlFunctionUtil.eval("DATE",arg));
    }

}
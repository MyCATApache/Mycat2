package io.mycat.calcite.sqlfunction;

import java.time.LocalTime;

/**
 * CURTIME
 * Syntax
 * CURTIME([precision])
 * Description
 * Returns the current time as a value in 'HH:MM:SS' or HHMMSS.uuuuuu format, depending on whether the function is used in a string or numeric context. The value is expressed in the current time zone.
 *
 * The optional precision determines the microsecond precision. See Microseconds in MariaDB.
 *
 * Examples
 * SELECT CURTIME();
 * +-----------+
 * | CURTIME() |
 * +-----------+
 * | 12:45:39  |
 * +-----------+
 *
 * SELECT CURTIME() + 0;
 * +---------------+
 * | CURTIME() + 0 |
 * +---------------+
 * | 124545.000000 |
 * +---------------+
 * With precision:
 *
 * SELECT CURTIME(2);
 * +-------------+
 * | CURTIME(2)  |
 * +-------------+
 * | 09:49:08.09 |
 * +-------------+
 */
public class CurTimeFunction {
    public static String eval() {
        return LocalTime.now().toString();
    }
}
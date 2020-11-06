package io.mycat.calcite.sqlfunction;

import java.time.LocalDate;
import java.util.Date;


/**
 * DAYOFMONTH
 * Syntax
 * DAYOFMONTH(date)
 * Description
 * Returns the day of the month for date, in the range 1 to 31, or 0 for dates such as '0000-00-00' or '2008-00-00' which have a zero day part.
 *
 * DAY() is a synonym.
 *
 * Examples
 * SELECT DAYOFMONTH('2007-02-03');
 * +--------------------------+
 * | DAYOFMONTH('2007-02-03') |
 * +--------------------------+
 * |                        3 |
 * +--------------------------+
 * CREATE TABLE t1 (d DATETIME);
 * INSERT INTO t1 VALUES
 *     ("2007-01-30 21:31:07"),
 *     ("1983-10-15 06:42:51"),
 *     ("2011-04-21 12:34:56"),
 *     ("2011-10-30 06:31:41"),
 *     ("2011-01-30 14:03:25"),
 *     ("2004-10-07 11:19:34");
 * SELECT d FROM t1 where DAYOFMONTH(d) = 30;
 * +---------------------+
 * | d                   |
 * +---------------------+
 * | 2007-01-30 21:31:07 |
 * | 2011-10-30 06:31:41 |
 * | 2011-01-30 14:03:25 |
 * +---------------------+
 *
 */
public class DayOfMonthFunction {
    public static Integer eval(Date date) {
        if (date == null){
            return null;
        }
      return  LocalDate.from(date.toInstant()).getDayOfMonth();
    }

}
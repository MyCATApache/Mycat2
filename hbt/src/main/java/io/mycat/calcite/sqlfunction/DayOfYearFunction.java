package io.mycat.calcite.sqlfunction;

import java.time.LocalDate;
import java.util.Date;


/**
 * DAYOFYEAR
 * Syntax
 * DAYOFYEAR(date)
 * Description
 * Returns the day of the year for date, in the range 1 to 366.
 *
 * Examples
 * SELECT DAYOFYEAR('2018-02-16');
 * +-------------------------+
 * | DAYOFYEAR('2018-02-16') |
 * +-------------------------+
 * |                      47 |
 * +-------------------------+
 */
public class DayOfYearFunction {
    public static Integer eval(Date date) {
        if (date == null){
            return null;
        }
        return  LocalDate.from(date.toInstant()).getDayOfYear();
    }
}
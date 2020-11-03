package io.mycat.calcite.sqlfunction;

import java.util.Date;

/**
 * DAY
 * Syntax
 * DAY(date)
 * Description
 * DAY() is a synonym for DAYOFMONTH().
 */
public class DayFunction {
    public static Integer eval(Date date) {
      return DayOfMonthFunction.eval(date);
    }

}
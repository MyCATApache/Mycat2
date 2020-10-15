package io.mycat.calcite.sqlfunction;

import io.mycat.hbt4.MycatContext;

import java.time.LocalDate;
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
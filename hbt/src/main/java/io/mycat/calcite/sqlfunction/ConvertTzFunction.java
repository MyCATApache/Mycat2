package io.mycat.calcite.sqlfunction;


import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.util.Objects;

/**
 * Syntax
 * CONVERT_TZ(dt,from_tz,to_tz)
 * Description
 * CONVERT_TZ() converts a datetime value dt from the time zone given by from_tz to the time zone given by to_tz and returns the resulting value.
 *
 * In order to use named time zones, such as GMT, MET or Africa/Johannesburg, the time_zone tables must be loaded (see mysql_tzinfo_to_sql).
 *
 * No conversion will take place if the value falls outside of the supported TIMESTAMP range ('1970-01-01 00:00:01' to '2038-01-19 05:14:07' UTC) when converted from from_tz to UTC.
 *
 * This function returns NULL if the arguments are invalid (or named time zones have not been loaded).
 *
 * See time zones for more information.
 *
 * Examples
 * SELECT CONVERT_TZ('2016-01-01 12:00:00','+00:00','+10:00');
 * +-----------------------------------------------------+
 * | CONVERT_TZ('2016-01-01 12:00:00','+00:00','+10:00') |
 * +-----------------------------------------------------+
 * | 2016-01-01 22:00:00                                 |
 * +-----------------------------------------------------+
 * Using named time zones (with the time zone tables loaded):
 *
 * SELECT CONVERT_TZ('2016-01-01 12:00:00','GMT','Africa/Johannesburg');
 * +---------------------------------------------------------------+
 * | CONVERT_TZ('2016-01-01 12:00:00','GMT','Africa/Johannesburg') |
 * +---------------------------------------------------------------+
 * | 2016-01-01 14:00:00                                           |
 * +---------------------------------------------------------------+
 * The value is out of the TIMESTAMP range, so no conversion takes place:
 *
 * SELECT CONVERT_TZ('1969-12-31 22:00:00','+00:00','+10:00');
 * +-----------------------------------------------------+
 * | CONVERT_TZ('1969-12-31 22:00:00','+00:00','+10:00') |
 * +-----------------------------------------------------+
 * | 1969-12-31 22:00:00                                 |
 * +-----------------------------------------------------+
 */
public class ConvertTzFunction {
    public static String eval(String dt,String from_tz,String to_tz) {
        if (dt == null||from_tz == null||to_tz == null){
            return null;
        }
        return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("CONVERT_TZ", dt,from_tz,to_tz)));

    }
}
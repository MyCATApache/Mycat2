package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;

public class MySQLDateFunctions {


    /**
     * ADDDATE
     * Syntax
     * ADDDATE(date,INTERVAL expr unit), ADDDATE(expr,days)
     * Description
     * When invoked with the INTERVAL form of the second argument, ADDDATE() is a synonym for DATE_ADD(). The related function SUBDATE() is a synonym for DATE_SUB(). For information on the INTERVAL unit argument, see the discussion for DATE_ADD().
     * <p>
     * When invoked with the days form of the second argument, MariaDB treats it as an integer number of days to be added to expr.
     * <p>
     * Examples
     * SELECT DATE_ADD('2008-01-02', INTERVAL 31 DAY);
     * +-----------------------------------------+
     * | DATE_ADD('2008-01-02', INTERVAL 31 DAY) |
     * +-----------------------------------------+
     * | 2008-02-02                              |
     * +-----------------------------------------+
     * <p>
     * SELECT ADDDATE('2008-01-02', INTERVAL 31 DAY);
     * +----------------------------------------+
     * | ADDDATE('2008-01-02', INTERVAL 31 DAY) |
     * +----------------------------------------+
     * | 2008-02-02                             |
     * +----------------------------------------+
     * SELECT ADDDATE('2008-01-02', 31);
     * +---------------------------+
     * | ADDDATE('2008-01-02', 31) |
     * +---------------------------+
     * | 2008-02-02                |
     * +---------------------------+
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * SELECT d, ADDDATE(d, 10) from t1;
     * +---------------------+---------------------+
     * | d                   | ADDDATE(d, 10)      |
     * +---------------------+---------------------+
     * | 2007-01-30 21:31:07 | 2007-02-09 21:31:07 |
     * | 1983-10-15 06:42:51 | 1983-10-25 06:42:51 |
     * | 2011-04-21 12:34:56 | 2011-05-01 12:34:56 |
     * | 2011-10-30 06:31:41 | 2011-11-09 06:31:41 |
     * | 2011-01-30 14:03:25 | 2011-02-09 14:03:25 |
     * | 2004-10-07 11:19:34 | 2004-10-17 11:19:34 |
     * +---------------------+---------------------+
     * <p>
     * SELECT d, ADDDATE(d, INTERVAL 10 HOUR) from t1;
     * +---------------------+------------------------------+
     * | d                   | ADDDATE(d, INTERVAL 10 HOUR) |
     * +---------------------+------------------------------+
     * | 2007-01-30 21:31:07 | 2007-01-31 07:31:07          |
     * | 1983-10-15 06:42:51 | 1983-10-15 16:42:51          |
     * | 2011-04-21 12:34:56 | 2011-04-21 22:34:56          |
     * | 2011-10-30 06:31:41 | 2011-10-30 16:31:41          |
     * | 2011-01-30 14:03:25 | 2011-01-31 00:03:25          |
     * | 2004-10-07 11:19:34 | 2004-10-07 21:19:34          |
     * +---------------------+------------------------------+
     *
     * @param arg0
     * @param time
     * @return
     */
    public static Date ADDDATE(Date arg0, Time time) {
        if (arg0 == null || time == null) {
            return null;
        }
        return new Date(arg0.getTime() + time.getTime());
    }

    public static Date ADDDATE(Date arg0, Integer days) {
        if (arg0 == null || days == null) {
            return null;
        }
        ZoneId zone = ZoneId.systemDefault();
        LocalDate plus = LocalDate.ofEpochDay(arg0.getTime()).plus(days, ChronoUnit.DAYS);
        Instant instant = plus.atStartOfDay().atZone(zone).toInstant();
        return Date.from(instant);
    }

    /**
     * ADDTIME
     * Syntax
     * ADDTIME(expr1,expr2)
     * Description
     * ADDTIME() adds expr2 to expr1 and returns the result. expr1 is a time or datetime expression, and expr2 is a time expression.
     * <p>
     * Examples
     * SELECT ADDTIME('2007-12-31 23:59:59.999999', '1 1:1:1.000002');
     * +---------------------------------------------------------+
     * | ADDTIME('2007-12-31 23:59:59.999999', '1 1:1:1.000002') |
     * +---------------------------------------------------------+
     * | 2008-01-02 01:01:01.000001                              |
     * +---------------------------------------------------------+
     * <p>
     * SELECT ADDTIME('01:00:00.999999', '02:00:00.999998');
     * +-----------------------------------------------+
     * | ADDTIME('01:00:00.999999', '02:00:00.999998') |
     * +-----------------------------------------------+
     * | 03:00:01.999997                               |
     * +-----------------------------------------------+
     *
     * @param arg0
     * @param arg1
     * @return
     */
    public static Time ADDTIME(Time arg0, Time arg1) {
        if (arg0 == null || arg1 == null) {
            return null;
        }
        return new Time(arg0.getTime() + arg1.getTime());
    }

    /**
     * CONVERT_TZ
     * Syntax
     * CONVERT_TZ(dt,from_tz,to_tz)
     * Description
     * CONVERT_TZ() converts a datetime value dt from the time zone given by from_tz to the time zone given by to_tz and returns the resulting value.
     * <p>
     * In order to use named time zones, such as GMT, MET or Africa/Johannesburg, the time_zone tables must be loaded (see mysql_tzinfo_to_sql).
     * <p>
     * No conversion will take place if the value falls outside of the supported TIMESTAMP range ('1970-01-01 00:00:01' to '2038-01-19 05:14:07' UTC) when converted from from_tz to UTC.
     * <p>
     * This function returns NULL if the arguments are invalid (or named time zones have not been loaded).
     * <p>
     * See time zones for more information.
     * <p>
     * Examples
     * SELECT CONVERT_TZ('2016-01-01 12:00:00','+00:00','+10:00');
     * +-----------------------------------------------------+
     * | CONVERT_TZ('2016-01-01 12:00:00','+00:00','+10:00') |
     * +-----------------------------------------------------+
     * | 2016-01-01 22:00:00                                 |
     * +-----------------------------------------------------+
     * Using named time zones (with the time zone tables loaded):
     * <p>
     * SELECT CONVERT_TZ('2016-01-01 12:00:00','GMT','Africa/Johannesburg');
     * +---------------------------------------------------------------+
     * | CONVERT_TZ('2016-01-01 12:00:00','GMT','Africa/Johannesburg') |
     * +---------------------------------------------------------------+
     * | 2016-01-01 14:00:00                                           |
     * +---------------------------------------------------------------+
     * The value is out of the TIMESTAMP range, so no conversion takes place:
     * <p>
     * SELECT CONVERT_TZ('1969-12-31 22:00:00','+00:00','+10:00');
     * +-----------------------------------------------------+
     * | CONVERT_TZ('1969-12-31 22:00:00','+00:00','+10:00') |
     * +-----------------------------------------------------+
     * | 1969-12-31 22:00:00                                 |
     * +-----------------------------------------------------+
     *
     * @param arg0
     * @param arg1
     * @param arg2
     * @return
     */
    public static String CONVERT_TZ(Date arg0, Date arg1, Date arg2) {
        if (arg0 == null || arg1 == null || arg2 == null) {
            return null;
        }
        return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("CONVERT_TZ", arg0, arg1, arg2)));
    }

    /**
     * CURDATE
     * Syntax
     * CURDATE()
     * Description
     * Returns the current date as a value in 'YYYY-MM-DD' or YYYYMMDD format, depending on whether the function is used in a string or numeric context.
     * <p>
     * Examples
     * SELECT CURDATE();
     * +------------+
     * | CURDATE()  |
     * +------------+
     * | 2019-03-05 |
     * +------------+
     * In a numeric context (note this is not performing date calculations):
     * <p>
     * SELECT CURDATE() +0;
     * +--------------+
     * | CURDATE() +0 |
     * +--------------+
     * |     20190305 |
     * +--------------+
     * Data calculation:
     * <p>
     * SELECT CURDATE() - INTERVAL 5 DAY;
     * +----------------------------+
     * | CURDATE() - INTERVAL 5 DAY |
     * +----------------------------+
     * | 2019-02-28                 |
     * +----------------------------+
     *
     * @return
     */
    public static Date CURDATE() {
        return new Date();
    }

    /**
     * CURRENT_DATE
     * Syntax
     * CURRENT_DATE, CURRENT_DATE()
     * Description
     * CURRENT_DATE and CURRENT_DATE() are synonyms for CURDATE().
     *
     * @return
     */
    public static Date CURRENT_DATE() {
        return CURDATE();
    }

    /**
     * CURRENT_TIME
     * Syntax
     * CURRENT_TIME
     * CURRENT_TIME([precision])
     * Description
     * CURRENT_TIME and CURRENT_TIME() are synonyms for CURTIME().
     *
     * @return
     */
    public static Time CURRENT_TIME() {
        return CURTIME();
    }

    /**
     * CURTIME
     * Syntax
     * CURTIME([precision])
     * Description
     * Returns the current time as a value in 'HH:MM:SS' or HHMMSS.uuuuuu format, depending on whether the function is used in a string or numeric context. The value is expressed in the current time zone.
     * <p>
     * The optional precision determines the microsecond precision. See Microseconds in MariaDB.
     * <p>
     * Examples
     * SELECT CURTIME();
     * +-----------+
     * | CURTIME() |
     * +-----------+
     * | 12:45:39  |
     * +-----------+
     * <p>
     * SELECT CURTIME() + 0;
     * +---------------+
     * | CURTIME() + 0 |
     * +---------------+
     * | 124545.000000 |
     * +---------------+
     * With precision:
     * <p>
     * SELECT CURTIME(2);
     * +-------------+
     * | CURTIME(2)  |
     * +-------------+
     * | 09:49:08.09 |
     * +-------------+
     *
     * @return
     */
    public static Time CURTIME() {
        return Time.valueOf(LocalTime.now());
    }

    public static Timestamp CURRENT_TIMESTAMP() {
        return NOW();
    }

    public static Timestamp NOW() {
        return new Timestamp(System.currentTimeMillis());
    }

    /**
     * DATE FUNCTION
     * Syntax
     * DATE(expr)
     * Description
     * Extracts the date part of the date or datetime expression expr.
     * <p>
     * Examples
     * SELECT DATE('2013-07-18 12:21:32');
     * +-----------------------------+
     * | DATE('2013-07-18 12:21:32') |
     * +-----------------------------+
     * | 2013-07-18                  |
     * +-----------------------------+
     */
    public static Integer DATEDIFF(Date arg0, Date arg1) {
        if (arg0 == null || arg1 == null) return null;
        Period between = Period.between(LocalDate.ofEpochDay(arg0.getTime()), LocalDate.ofEpochDay(arg1.getTime()));
        return between.getDays();
    }

    /**
     * DATE_ADD
     * Syntax
     * DATE_ADD(date,INTERVAL expr unit)
     * Description
     * Performs date arithmetic. The date argument specifies the starting date or datetime value. expr is an expression specifying the interval value to be added or subtracted from the starting date. expr is a string; it may start with a "-" for negative intervals. unit is a keyword indicating the units in which the expression should be interpreted. See Date and Time Units for a complete list of permitted units.
     * <p>
     * See also DATE_SUB().
     * <p>
     * Examples
     * SELECT '2008-12-31 23:59:59' + INTERVAL 1 SECOND;
     * +-------------------------------------------+
     * | '2008-12-31 23:59:59' + INTERVAL 1 SECOND |
     * +-------------------------------------------+
     * | 2009-01-01 00:00:00                       |
     * +-------------------------------------------+
     * SELECT INTERVAL 1 DAY + '2008-12-31';
     * +-------------------------------+
     * | INTERVAL 1 DAY + '2008-12-31' |
     * +-------------------------------+
     * | 2009-01-01                    |
     * +-------------------------------+
     * SELECT '2005-01-01' - INTERVAL 1 SECOND;
     * +----------------------------------+
     * | '2005-01-01' - INTERVAL 1 SECOND |
     * +----------------------------------+
     * | 2004-12-31 23:59:59              |
     * +----------------------------------+
     * SELECT DATE_ADD('2000-12-31 23:59:59', INTERVAL 1 SECOND);
     * +----------------------------------------------------+
     * | DATE_ADD('2000-12-31 23:59:59', INTERVAL 1 SECOND) |
     * +----------------------------------------------------+
     * | 2001-01-01 00:00:00                                |
     * +----------------------------------------------------+
     * SELECT DATE_ADD('2010-12-31 23:59:59', INTERVAL 1 DAY);
     * +-------------------------------------------------+
     * | DATE_ADD('2010-12-31 23:59:59', INTERVAL 1 DAY) |
     * +-------------------------------------------------+
     * | 2011-01-01 23:59:59                             |
     * +-------------------------------------------------+
     * SELECT DATE_ADD('2100-12-31 23:59:59', INTERVAL '1:1' MINUTE_SECOND);
     * +---------------------------------------------------------------+
     * | DATE_ADD('2100-12-31 23:59:59', INTERVAL '1:1' MINUTE_SECOND) |
     * +---------------------------------------------------------------+
     * | 2101-01-01 00:01:00                                           |
     * +---------------------------------------------------------------+
     * SELECT DATE_ADD('1900-01-01 00:00:00', INTERVAL '-1 10' DAY_HOUR);
     * +------------------------------------------------------------+
     * | DATE_ADD('1900-01-01 00:00:00', INTERVAL '-1 10' DAY_HOUR) |
     * +------------------------------------------------------------+
     * | 1899-12-30 14:00:00                                        |
     * +------------------------------------------------------------+
     * SELECT DATE_ADD('1992-12-31 23:59:59.000002', INTERVAL '1.999999' SECOND_MICROSECOND);
     * +--------------------------------------------------------------------------------+
     * | DATE_ADD('1992-12-31 23:59:59.000002', INTERVAL '1.999999' SECOND_MICROSECOND) |
     * +--------------------------------------------------------------------------------+
     * | 1993-01-01 00:00:01.000001                                                     |
     * +--------------------------------------------------------------------------------+
     *
     * @param arg0
     * @param arg1
     * @return
     */
    public static Date DATEADD(Date arg0, Time arg1) {
        if (arg0 == null || arg1 == null) return null;
        return new Date(arg0.getTime() + arg1.getTime());
    }


    /**
     * DATE_FORMAT
     * Syntax
     * DATE_FORMAT(date, format[, locale])
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Formats the date value according to the format string.
     * <p>
     * The language used for the names is controlled by the value of the lc_time_names system variable. See server locale for more on the supported locales.
     * <p>
     * The options that can be used by DATE_FORMAT(), as well as its inverse STR_TO_DATE() and the FROM_UNIXTIME() function, are:
     * <p>
     * Option	Description
     * %a	Short weekday name in current locale (Variable lc_time_names).
     * %b	Short form month name in current locale. For locale en_US this is one of: Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov or Dec.
     * %c	Month with 1 or 2 digits.
     * %D	Day with English suffix 'th', 'nd', 'st' or 'rd''. (1st, 2nd, 3rd...).
     * %d	Day with 2 digits.
     * %e	Day with 1 or 2 digits.
     * %f	Sub seconds 6 digits.
     * %H	Hour with 2 digits between 00-23.
     * %h	Hour with 2 digits between 01-12.
     * %I	Hour with 2 digits between 01-12.
     * %i	Minute with 2 digits.
     * %j	Day of the year (001-366)
     * %k	Hour with 1 digits between 0-23.
     * %l	Hour with 1 digits between 1-12.
     * %M	Full month name in current locale (Variable lc_time_names).
     * %m	Month with 2 digits.
     * %p	AM/PM according to current locale (Variable lc_time_names).
     * %r	Time in 12 hour format, followed by AM/PM. Short for '%I:%i:%S %p'.
     * %S	Seconds with 2 digits.
     * %s	Seconds with 2 digits.
     * %T	Time in 24 hour format. Short for '%H:%i:%S'.
     * %U	Week number (00-53), when first day of the week is Sunday.
     * %u	Week number (00-53), when first day of the week is Monday.
     * %V	Week number (01-53), when first day of the week is Sunday. Used with %X.
     * %v	Week number (01-53), when first day of the week is Monday. Used with %x.
     * %W	Full weekday name in current locale (Variable lc_time_names).
     * %w	Day of the week. 0 = Sunday, 6 = Saturday.
     * %X	Year with 4 digits when first day of the week is Sunday. Used with %V.
     * %x	Year with 4 digits when first day of the week is Monday. Used with %v.
     * %Y	Year with 4 digits.
     * %y	Year with 2 digits.
     * %#	For str_to_date(), skip all numbers.
     * %.	For str_to_date(), skip all punctation characters.
     * %@	For str_to_date(), skip all alpha characters.
     * %%	A literal % character.
     * To get a date in one of the standard formats, GET_FORMAT() can be used.
     * <p>
     * Examples
     * SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y');
     * +------------------------------------------------+
     * | DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y') |
     * +------------------------------------------------+
     * | Sunday October 2009                            |
     * +------------------------------------------------+
     * <p>
     * SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s');
     * +------------------------------------------------+
     * | DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s') |
     * +------------------------------------------------+
     * | 22:23:00                                       |
     * +------------------------------------------------+
     * <p>
     * SELECT DATE_FORMAT('1900-10-04 22:23:00', '%D %y %a %d %m %b %j');
     * +------------------------------------------------------------+
     * | DATE_FORMAT('1900-10-04 22:23:00', '%D %y %a %d %m %b %j') |
     * +------------------------------------------------------------+
     * | 4th 00 Thu 04 10 Oct 277                                   |
     * +------------------------------------------------------------+
     * <p>
     * SELECT DATE_FORMAT('1997-10-04 22:23:00', '%H %k %I %r %T %S %w');
     * +------------------------------------------------------------+
     * | DATE_FORMAT('1997-10-04 22:23:00', '%H %k %I %r %T %S %w') |
     * +------------------------------------------------------------+
     * | 22 22 10 10:23:00 PM 22:23:00 00 6                         |
     * +------------------------------------------------------------+
     * <p>
     * SELECT DATE_FORMAT('1999-01-01', '%X %V');
     * +------------------------------------+
     * | DATE_FORMAT('1999-01-01', '%X %V') |
     * +------------------------------------+
     * | 1998 52                            |
     * +------------------------------------+
     * <p>
     * SELECT DATE_FORMAT('2006-06-00', '%d');
     * +---------------------------------+
     * | DATE_FORMAT('2006-06-00', '%d') |
     * +---------------------------------+
     * | 00                              |
     * +---------------------------------+
     * MariaDB starting with 10.3.2
     * Optionally, the locale can be explicitly specified as the third DATE_FORMAT() argument. Doing so makes the function independent from the session settings, and the three argument version of DATE_FORMAT() can be used in virtual indexed and persistent generated-columns:
     * <p>
     * SELECT DATE_FORMAT('2006-01-01', '%W', 'el_GR');
     * +------------------------------------------+
     * | DATE_FORMAT('2006-01-01', '%W', 'el_GR') |
     * +------------------------------------------+
     * | Κυριακή                                  |
     * +------------------------------------------+
     *
     * @param arg0
     * @param format
     * @return
     */
    public static String DATE_FORMAT(Date arg0, String format) {
        if (arg0 == null || format == null) return null;
        return Objects.toString(UnsolvedMysqlFunctionUtil.eval("DATE_FORMAT", arg0, format));
    }

    public static String DATE_FORMAT(Date arg0, String format, String locale) {
        if (arg0 == null || format == null || locale == null) return null;
        return Objects.toString(UnsolvedMysqlFunctionUtil.eval("DATE_FORMAT", arg0, format, locale));
    }

    /**
     * DATE_SUB
     * Syntax
     * DATE_SUB(date,INTERVAL expr unit)
     * Description
     * Performs date arithmetic. The date argument specifies the starting date or datetime value. expr is an expression specifying the interval value to be added or subtracted from the starting date. expr is a string; it may start with a "-" for negative intervals. unit is a keyword indicating the units in which the expression should be interpreted. See Date and Time Units for a complete list of permitted units.
     * <p>
     * See also DATE_ADD().
     * <p>
     * Examples
     * SELECT DATE_SUB('1998-01-02', INTERVAL 31 DAY);
     * +-----------------------------------------+
     * | DATE_SUB('1998-01-02', INTERVAL 31 DAY) |
     * +-----------------------------------------+
     * | 1997-12-02                              |
     * +-----------------------------------------+
     * SELECT DATE_SUB('2005-01-01 00:00:00', INTERVAL '1 1:1:1' DAY_SECOND);
     * +----------------------------------------------------------------+
     * | DATE_SUB('2005-01-01 00:00:00', INTERVAL '1 1:1:1' DAY_SECOND) |
     * +----------------------------------------------------------------+
     * | 2004-12-30 22:58:59                                            |
     * +----------------------------------------------------------------+
     *
     * @param arg0
     * @param arg1
     * @return
     */
    public static Date DATE_SUB(Date arg0, Time arg1) {
        if (arg0 == null || arg1 == null) return null;
        return new Date(arg0.getTime() - arg1.getTime());
    }

    /**
     * DAY
     * Syntax
     * DAY(date)
     * Description
     * DAY() is a synonym for DAYOFMONTH().
     *
     * @param arg0
     * @return
     */
    public static Integer day(Date arg0) {
        if (arg0 == null) return null;
        return DAYOFMONTH(arg0);
    }

    /**
     * DAYNAME
     * Syntax
     * DAYNAME(date)
     * Description
     * Returns the name of the weekday for date. The language used for the name is controlled by the value of the lc_time_names system variable. See server locale for more on the supported locales.
     * <p>
     * Examples
     * SELECT DAYNAME('2007-02-03');
     * +-----------------------+
     * | DAYNAME('2007-02-03') |
     * +-----------------------+
     * | Saturday              |
     * +-----------------------+
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * SELECT d, DAYNAME(d) FROM t1;
     * +---------------------+------------+
     * | d                   | DAYNAME(d) |
     * +---------------------+------------+
     * | 2007-01-30 21:31:07 | Tuesday    |
     * | 1983-10-15 06:42:51 | Saturday   |
     * | 2011-04-21 12:34:56 | Thursday   |
     * | 2011-10-30 06:31:41 | Sunday     |
     * | 2011-01-30 14:03:25 | Sunday     |
     * | 2004-10-07 11:19:34 | Thursday   |
     * +---------------------+------------+
     * Changing the locale:
     * <p>
     * SET lc_time_names = 'fr_CA';
     * <p>
     * SELECT DAYNAME('2013-04-01');
     * +-----------------------+
     * | DAYNAME('2013-04-01') |
     * +-----------------------+
     * | lundi                 |
     * +-----------------------+
     *
     * @param arg0
     * @return
     */
    public static String DAYNAME(Date arg0) {
        if (arg0 == null) return null;
        return LocalDate.ofEpochDay(arg0.getTime()).getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.getDefault());
    }

    /**
     * DAYOFMONTH
     * Syntax
     * DAYOFMONTH(date)
     * Description
     * Returns the day of the month for date, in the range 1 to 31, or 0 for dates such as '0000-00-00' or '2008-00-00' which have a zero day part.
     * <p>
     * DAY() is a synonym.
     * <p>
     * Examples
     * SELECT DAYOFMONTH('2007-02-03');
     * +--------------------------+
     * | DAYOFMONTH('2007-02-03') |
     * +--------------------------+
     * |                        3 |
     * +--------------------------+
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * SELECT d FROM t1 where DAYOFMONTH(d) = 30;
     * +---------------------+
     * | d                   |
     * +---------------------+
     * | 2007-01-30 21:31:07 |
     * | 2011-10-30 06:31:41 |
     * | 2011-01-30 14:03:25 |
     * +---------------------+
     *
     * @param arg0
     * @return
     */
    public static Integer DAYOFMONTH(Date arg0) {
        if (arg0 == null) return null;
        return LocalDate.ofEpochDay(arg0.getTime()).getDayOfMonth();
    }

    /**
     * DAYOFWEEK
     * Syntax
     * DAYOFWEEK(date)
     * Description
     * Returns the day of the week index for the date (1 = Sunday, 2 = Monday, ..., 7 = Saturday). These index values correspond to the ODBC standard.
     * <p>
     * This contrasts with WEEKDAY() which follows a different index numbering (0 = Monday, 1 = Tuesday, ... 6 = Sunday).
     * <p>
     * Examples
     * SELECT DAYOFWEEK('2007-02-03');
     * +-------------------------+
     * | DAYOFWEEK('2007-02-03') |
     * +-------------------------+
     * |                       7 |
     * +-------------------------+
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * SELECT d, DAYNAME(d), DAYOFWEEK(d), WEEKDAY(d) from t1;
     * +---------------------+------------+--------------+------------+
     * | d                   | DAYNAME(d) | DAYOFWEEK(d) | WEEKDAY(d) |
     * +---------------------+------------+--------------+------------+
     * | 2007-01-30 21:31:07 | Tuesday    |            3 |          1 |
     * | 1983-10-15 06:42:51 | Saturday   |            7 |          5 |
     * | 2011-04-21 12:34:56 | Thursday   |            5 |          3 |
     * | 2011-10-30 06:31:41 | Sunday     |            1 |          6 |
     * | 2011-01-30 14:03:25 | Sunday     |            1 |          6 |
     * | 2004-10-07 11:19:34 | Thursday   |            5 |          3 |
     * +---------------------+------------+--------------+------------+
     *
     * @param arg0
     * @return
     */
    public static Integer DAYOFWEEK(Date arg0) {
        if (arg0 == null) return null;
        return LocalDate.ofEpochDay(arg0.getTime()).getDayOfWeek().getValue();
    }

    /**
     * DAYOFYEAR
     * Syntax
     * DAYOFYEAR(date)
     * Description
     * Returns the day of the year for date, in the range 1 to 366.
     * <p>
     * Examples
     * SELECT DAYOFYEAR('2018-02-16');
     * +-------------------------+
     * | DAYOFYEAR('2018-02-16') |
     * +-------------------------+
     * |                      47 |
     * +-------------------------+
     *
     * @param arg0
     * @return
     */
    public static Integer DAYOFYEAR(Date arg0) {
        if (arg0 == null) return null;
        return arg0.getYear();
    }
    //EXTRACT

    /**
     * FROM_DAYS
     * Syntax
     * FROM_DAYS(N)
     * Description
     * Given a day number N, returns a DATE value. The day count is based on the number of days from the start of the standard calendar (0000-00-00).
     * <p>
     * The function is not designed for use with dates before the advent of the Gregorian calendar in October 1582. Results will not be reliable since it doesn't account for the lost days when the calendar changed from the Julian calendar.
     * <p>
     * This is the converse of the TO_DAYS() function.
     * <p>
     * Examples
     * SELECT FROM_DAYS(730669);
     * +-------------------+
     * | FROM_DAYS(730669) |
     * +-------------------+
     * | 2000-07-03        |
     * +-------------------+
     *
     * @param arg0
     * @return
     */
    public static Date FROM_DAYS(Long arg0) {
        if (arg0 == null) return null;
        return new Date(arg0);
    }

    /**
     * FROM_UNIXTIME
     * Syntax
     * FROM_UNIXTIME(unix_timestamp), FROM_UNIXTIME(unix_timestamp,format)
     * Contents
     * Syntax
     * Description
     * Performance Considerations
     * Examples
     * See Also
     * Description
     * Returns a representation of the unix_timestamp argument as a value in 'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS.uuuuuu format, depending on whether the function is used in a string or numeric context. The value is expressed in the current time zone. unix_timestamp is an internal timestamp value such as is produced by the UNIX_TIMESTAMP() function.
     * <p>
     * If format is given, the result is formatted according to the format string, which is used the same way as listed in the entry for the DATE_FORMAT() function.
     * <p>
     * Timestamps in MariaDB have a maximum value of 2147483647, equivalent to 2038-01-19 05:14:07. This is due to the underlying 32-bit limitation. Using the function on a timestamp beyond this will result in NULL being returned. Use DATETIME as a storage type if you require dates beyond this.
     * <p>
     * The options that can be used by FROM_UNIXTIME(), as well as DATE_FORMAT() and STR_TO_DATE(), are:
     * <p>
     * Option	Description
     * %a	Short weekday name in current locale (Variable lc_time_names).
     * %b	Short form month name in current locale. For locale en_US this is one of: Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov or Dec.
     * %c	Month with 1 or 2 digits.
     * %D	Day with English suffix 'th', 'nd', 'st' or 'rd''. (1st, 2nd, 3rd...).
     * %d	Day with 2 digits.
     * %e	Day with 1 or 2 digits.
     * %f	Sub seconds 6 digits.
     * %H	Hour with 2 digits between 00-23.
     * %h	Hour with 2 digits between 01-12.
     * %I	Hour with 2 digits between 01-12.
     * %i	Minute with 2 digits.
     * %j	Day of the year (001-366)
     * %k	Hour with 1 digits between 0-23.
     * %l	Hour with 1 digits between 1-12.
     * %M	Full month name in current locale (Variable lc_time_names).
     * %m	Month with 2 digits.
     * %p	AM/PM according to current locale (Variable lc_time_names).
     * %r	Time in 12 hour format, followed by AM/PM. Short for '%I:%i:%S %p'.
     * %S	Seconds with 2 digits.
     * %s	Seconds with 2 digits.
     * %T	Time in 24 hour format. Short for '%H:%i:%S'.
     * %U	Week number (00-53), when first day of the week is Sunday.
     * %u	Week number (00-53), when first day of the week is Monday.
     * %V	Week number (01-53), when first day of the week is Sunday. Used with %X.
     * %v	Week number (01-53), when first day of the week is Monday. Used with %x.
     * %W	Full weekday name in current locale (Variable lc_time_names).
     * %w	Day of the week. 0 = Sunday, 1 = Saturday.
     * %X	Year with 4 digits when first day of the week is Sunday. Used with %V.
     * %x	Year with 4 digits when first day of the week is Sunday. Used with %v.
     * %Y	Year with 4 digits.
     * %y	Year with 2 digits.
     * %#	For str_to_date(), skip all numbers.
     * %.	For str_to_date(), skip all punctation characters.
     * %@	For str_to_date(), skip all alpha characters.
     * %%	A literal % character.
     * Performance Considerations
     * If your session time zone is set to SYSTEM (the default), FROM_UNIXTIME() will call the OS function to convert the data using the system time zone. At least on Linux, the corresponding function (localtime_r) uses a global mutex inside glibc that can cause contention under high concurrent load.
     * <p>
     * Set your time zone to a named time zone to avoid this issue. See mysql time zone tables for details on how to do this.
     * <p>
     * Examples
     * SELECT FROM_UNIXTIME(1196440219);
     * +---------------------------+
     * | FROM_UNIXTIME(1196440219) |
     * +---------------------------+
     * | 2007-11-30 11:30:19       |
     * +---------------------------+
     * <p>
     * SELECT FROM_UNIXTIME(1196440219) + 0;
     * +-------------------------------+
     * | FROM_UNIXTIME(1196440219) + 0 |
     * +-------------------------------+
     * |         20071130113019.000000 |
     * +-------------------------------+
     * <p>
     * SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(), '%Y %D %M %h:%i:%s %x');
     * +---------------------------------------------------------+
     * | FROM_UNIXTIME(UNIX_TIMESTAMP(), '%Y %D %M %h:%i:%s %x') |
     * +---------------------------------------------------------+
     * | 2010 27th March 01:03:47 2010                           |
     * +---------------------------------------------------------+
     * See Also
     * UNIX_TIMESTAMP()
     * DATE_FORMAT()
     * STR_TO_DATE()
     *
     * @param unix_timestamp
     * @return
     */
    public static String FROM_UNIXTIME(String unix_timestamp) {
        if (unix_timestamp == null) return null;
        return UnsolvedMysqlFunctionUtil.eval("FROM_UNIXTIME", unix_timestamp).toString();
    }

    /**
     * FROM_UNIXTIME
     * Syntax
     * FROM_UNIXTIME(unix_timestamp), FROM_UNIXTIME(unix_timestamp,format)
     * Contents
     * Syntax
     * Description
     * Performance Considerations
     * Examples
     * See Also
     * Description
     * Returns a representation of the unix_timestamp argument as a value in 'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS.uuuuuu format, depending on whether the function is used in a string or numeric context. The value is expressed in the current time zone. unix_timestamp is an internal timestamp value such as is produced by the UNIX_TIMESTAMP() function.
     * <p>
     * If format is given, the result is formatted according to the format string, which is used the same way as listed in the entry for the DATE_FORMAT() function.
     * <p>
     * Timestamps in MariaDB have a maximum value of 2147483647, equivalent to 2038-01-19 05:14:07. This is due to the underlying 32-bit limitation. Using the function on a timestamp beyond this will result in NULL being returned. Use DATETIME as a storage type if you require dates beyond this.
     * <p>
     * The options that can be used by FROM_UNIXTIME(), as well as DATE_FORMAT() and STR_TO_DATE(), are:
     * <p>
     * Option	Description
     * %a	Short weekday name in current locale (Variable lc_time_names).
     * %b	Short form month name in current locale. For locale en_US this is one of: Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov or Dec.
     * %c	Month with 1 or 2 digits.
     * %D	Day with English suffix 'th', 'nd', 'st' or 'rd''. (1st, 2nd, 3rd...).
     * %d	Day with 2 digits.
     * %e	Day with 1 or 2 digits.
     * %f	Sub seconds 6 digits.
     * %H	Hour with 2 digits between 00-23.
     * %h	Hour with 2 digits between 01-12.
     * %I	Hour with 2 digits between 01-12.
     * %i	Minute with 2 digits.
     * %j	Day of the year (001-366)
     * %k	Hour with 1 digits between 0-23.
     * %l	Hour with 1 digits between 1-12.
     * %M	Full month name in current locale (Variable lc_time_names).
     * %m	Month with 2 digits.
     * %p	AM/PM according to current locale (Variable lc_time_names).
     * %r	Time in 12 hour format, followed by AM/PM. Short for '%I:%i:%S %p'.
     * %S	Seconds with 2 digits.
     * %s	Seconds with 2 digits.
     * %T	Time in 24 hour format. Short for '%H:%i:%S'.
     * %U	Week number (00-53), when first day of the week is Sunday.
     * %u	Week number (00-53), when first day of the week is Monday.
     * %V	Week number (01-53), when first day of the week is Sunday. Used with %X.
     * %v	Week number (01-53), when first day of the week is Monday. Used with %x.
     * %W	Full weekday name in current locale (Variable lc_time_names).
     * %w	Day of the week. 0 = Sunday, 1 = Saturday.
     * %X	Year with 4 digits when first day of the week is Sunday. Used with %V.
     * %x	Year with 4 digits when first day of the week is Sunday. Used with %v.
     * %Y	Year with 4 digits.
     * %y	Year with 2 digits.
     * %#	For str_to_date(), skip all numbers.
     * %.	For str_to_date(), skip all punctation characters.
     * %@	For str_to_date(), skip all alpha characters.
     * %%	A literal % character.
     * Performance Considerations
     * If your session time zone is set to SYSTEM (the default), FROM_UNIXTIME() will call the OS function to convert the data using the system time zone. At least on Linux, the corresponding function (localtime_r) uses a global mutex inside glibc that can cause contention under high concurrent load.
     * <p>
     * Set your time zone to a named time zone to avoid this issue. See mysql time zone tables for details on how to do this.
     * <p>
     * Examples
     * SELECT FROM_UNIXTIME(1196440219);
     * +---------------------------+
     * | FROM_UNIXTIME(1196440219) |
     * +---------------------------+
     * | 2007-11-30 11:30:19       |
     * +---------------------------+
     * <p>
     * SELECT FROM_UNIXTIME(1196440219) + 0;
     * +-------------------------------+
     * | FROM_UNIXTIME(1196440219) + 0 |
     * +-------------------------------+
     * |         20071130113019.000000 |
     * +-------------------------------+
     * <p>
     * SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(), '%Y %D %M %h:%i:%s %x');
     * +---------------------------------------------------------+
     * | FROM_UNIXTIME(UNIX_TIMESTAMP(), '%Y %D %M %h:%i:%s %x') |
     * +---------------------------------------------------------+
     * | 2010 27th March 01:03:47 2010                           |
     * +---------------------------------------------------------+
     *
     * @param unix_timestamp
     * @param format
     * @return
     */
    public static String FROM_UNIXTIME(String unix_timestamp, String format) {
        if (unix_timestamp == null) return null;
        return UnsolvedMysqlFunctionUtil.eval("FROM_UNIXTIME", unix_timestamp, format).toString();
    }

    /**
     * GET_FORMAT
     * Syntax
     * GET_FORMAT({DATE|DATETIME|TIME}, {'EUR'|'USA'|'JIS'|'ISO'|'INTERNAL'})
     * Description
     * Returns a format string. This function is useful in combination with the DATE_FORMAT() and the STR_TO_DATE() functions.
     * <p>
     * Possible result formats are:
     * <p>
     * Function Call	Result Format
     * GET_FORMAT(DATE,'EUR')	'%d.%m.%Y'
     * GET_FORMAT(DATE,'USA')	'%m.%d.%Y'
     * GET_FORMAT(DATE,'JIS')	'%Y-%m-%d'
     * GET_FORMAT(DATE,'ISO')	'%Y-%m-%d'
     * GET_FORMAT(DATE,'INTERNAL')	'%Y%m%d'
     * GET_FORMAT(DATETIME,'EUR')	'%Y-%m-%d %H.%i.%s'
     * GET_FORMAT(DATETIME,'USA')	'%Y-%m-%d %H.%i.%s'
     * GET_FORMAT(DATETIME,'JIS')	'%Y-%m-%d %H:%i:%s'
     * GET_FORMAT(DATETIME,'ISO')	'%Y-%m-%d %H:%i:%s'
     * GET_FORMAT(DATETIME,'INTERNAL')	'%Y%m%d%H%i%s'
     * GET_FORMAT(TIME,'EUR')	'%H.%i.%s'
     * GET_FORMAT(TIME,'USA')	'%h:%i:%s %p'
     * GET_FORMAT(TIME,'JIS')	'%H:%i:%s'
     * GET_FORMAT(TIME,'ISO')	'%H:%i:%s'
     * GET_FORMAT(TIME,'INTERNAL')	'%H%i%s'
     * Examples
     * Obtaining the string matching to the standard European date format:
     * <p>
     * SELECT GET_FORMAT(DATE, 'EUR');
     * +-------------------------+
     * | GET_FORMAT(DATE, 'EUR') |
     * +-------------------------+
     * | %d.%m.%Y                |
     * +-------------------------+
     * Using the same string to format a date:
     * <p>
     * SELECT DATE_FORMAT('2003-10-03',GET_FORMAT(DATE,'EUR'));
     * +--------------------------------------------------+
     * | DATE_FORMAT('2003-10-03',GET_FORMAT(DATE,'EUR')) |
     * +--------------------------------------------------+
     * | 03.10.2003                                       |
     * +--------------------------------------------------+
     * <p>
     * SELECT STR_TO_DATE('10.31.2003',GET_FORMAT(DATE,'USA'));
     * +--------------------------------------------------+
     * | STR_TO_DATE('10.31.2003',GET_FORMAT(DATE,'USA')) |
     * +--------------------------------------------------+
     * | 2003-10-31                                       |
     * +--------------------------------------------------+
     *
     * @param arg0
     * @param arg1
     * @return
     */
    public static String GET_FORMAT(String arg0, String arg1) {
        return UnsolvedMysqlFunctionUtil.eval("GET_FORMAT", arg0, arg1).toString();
    }

    /**
     * HOUR
     * Syntax
     * HOUR(time)
     * Description
     * Returns the hour for time. The range of the return value is 0 to 23 for time-of-day values. However, the range of TIME values actually is much larger, so HOUR can return values greater than 23.
     * <p>
     * The return value is always positive, even if a negative TIME value is provided.
     * <p>
     * Examples
     * SELECT HOUR('10:05:03');
     * +------------------+
     * | HOUR('10:05:03') |
     * +------------------+
     * |               10 |
     * +------------------+
     * <p>
     * SELECT HOUR('272:59:59');
     * +-------------------+
     * | HOUR('272:59:59') |
     * +-------------------+
     * |               272 |
     * +-------------------+
     * Difference between EXTRACT (HOUR FROM ...) (>= MariaDB 10.0.7 and MariaDB 5.5.35) and HOUR:
     * <p>
     * SELECT EXTRACT(HOUR FROM '26:30:00'), HOUR('26:30:00');
     * +-------------------------------+------------------+
     * | EXTRACT(HOUR FROM '26:30:00') | HOUR('26:30:00') |
     * +-------------------------------+------------------+
     * |                             2 |               26 |
     * +-------------------------------+------------------+
     *
     * @param arg0
     * @return
     */
    public static Integer HOUR(Time arg0) {
        if (arg0 == null) {
            return null;
        }
        return arg0.getHours();
    }

    /**
     * LAST_DAY
     * Syntax
     * LAST_DAY(date)
     * Description
     * Takes a date or datetime value and returns the corresponding value for the last day of the month. Returns NULL if the argument is invalid.
     * <p>
     * Examples
     * SELECT LAST_DAY('2003-02-05');
     * +------------------------+
     * | LAST_DAY('2003-02-05') |
     * +------------------------+
     * | 2003-02-28             |
     * +------------------------+
     * <p>
     * SELECT LAST_DAY('2004-02-05');
     * +------------------------+
     * | LAST_DAY('2004-02-05') |
     * +------------------------+
     * | 2004-02-29             |
     * +------------------------+
     * <p>
     * SELECT LAST_DAY('2004-01-01 01:01:01');
     * +---------------------------------+
     * | LAST_DAY('2004-01-01 01:01:01') |
     * +---------------------------------+
     * | 2004-01-31                      |
     * +---------------------------------+
     * <p>
     * SELECT LAST_DAY('2003-03-32');
     * +------------------------+
     * | LAST_DAY('2003-03-32') |
     * +------------------------+
     * | NULL                   |
     * +------------------------+
     * 1 row in set, 1 warning (0.00 sec)
     * <p>
     * Warning (Code 1292): Incorrect datetime value: '2003-03-32'
     *
     * @param arg0
     * @return
     */
    public static Date LAST_DAY(Date arg0) {
        if (arg0 == null) {
            return null;
        }
        int month = arg0.getMonth();
        int year = arg0.getYear();
        long l = LocalDate.of(year, month, 1).plus(Period.ofMonths(1)).minus(Period.ofDays(1)).toEpochDay();
        return new Date(l);
    }

    /**
     * LOCALTIME
     * Syntax
     * LOCALTIME
     * LOCALTIME([precision])
     * Description
     * LOCALTIME and LOCALTIME() are synonyms for NOW().
     * <p>
     * See Also
     *
     * @return
     */
    public static Timestamp LOCALTIME() {
        return NOW();
    }

    /**
     * Open Questions
     * MariaDB Server
     * MariaDB MaxScale
     * MariaDB ColumnStore
     * Connectors
     * Created
     * 10 years, 5 months ago
     * Modified
     * 2 years, 2 months ago
     * Type
     * article
     * Status
     * active
     * License
     * GPLv2 fill_help_tables.sql
     * History
     * Comments
     * Edit
     * Attachments
     * No attachments exist
     * Localized Versions
     * LOCALTIMESTAMP [it]
     * LOCALTIMESTAMP
     * Syntax
     * LOCALTIMESTAMP
     * LOCALTIMESTAMP([precision])
     * Description
     * LOCALTIMESTAMP and LOCALTIMESTAMP() are synonyms for NOW().
     *
     * @return
     */
    public static Timestamp LOCALTIMESTAMP() {
        return NOW();
    }


    /**
     * MAKEDATE
     * Syntax
     * MAKEDATE(year,dayofyear)
     * Description
     * Returns a date, given year and day-of-year values. dayofyear must be greater than 0 or the result is NULL.
     * <p>
     * Examples
     * SELECT MAKEDATE(2011,31), MAKEDATE(2011,32);
     * +-------------------+-------------------+
     * | MAKEDATE(2011,31) | MAKEDATE(2011,32) |
     * +-------------------+-------------------+
     * | 2011-01-31        | 2011-02-01        |
     * +-------------------+-------------------+
     * <p>
     * SELECT MAKEDATE(2011,365), MAKEDATE(2014,365);
     * +--------------------+--------------------+
     * | MAKEDATE(2011,365) | MAKEDATE(2014,365) |
     * +--------------------+--------------------+
     * | 2011-12-31         | 2014-12-31         |
     * +--------------------+--------------------+
     * <p>
     * SELECT MAKEDATE(2011,0);
     * +------------------+
     * | MAKEDATE(2011,0) |
     * +------------------+
     * | NULL             |
     * +------------------+
     *
     * @param year
     * @param dayofyear
     * @return
     */
    public static Date MAKEDATE(int year, int dayofyear) {
        return new Date(LocalDate.ofYearDay(year, dayofyear).toEpochDay());
    }

    /**
     * MAKETIME
     * Syntax
     * MAKETIME(hour,minute,second)
     * Description
     * Returns a time value calculated from the hour, minute, and second arguments.
     * <p>
     * If minute or second are out of the range 0 to 60, NULL is returned. The hour can be in the range -838 to 838, outside of which the value is truncated with a warning.
     * <p>
     * Examples
     * SELECT MAKETIME(13,57,33);
     * +--------------------+
     * | MAKETIME(13,57,33) |
     * +--------------------+
     * | 13:57:33           |
     * +--------------------+
     * <p>
     * SELECT MAKETIME(-13,57,33);
     * +---------------------+
     * | MAKETIME(-13,57,33) |
     * +---------------------+
     * | -13:57:33           |
     * +---------------------+
     * <p>
     * SELECT MAKETIME(13,67,33);
     * +--------------------+
     * | MAKETIME(13,67,33) |
     * +--------------------+
     * | NULL               |
     * +--------------------+
     * <p>
     * SELECT MAKETIME(-1000,57,33);
     * +-----------------------+
     * | MAKETIME(-1000,57,33) |
     * +-----------------------+
     * | -838:59:59            |
     * +-----------------------+
     * 1 row in set, 1 warning (0.00 sec)
     * <p>
     * SHOW WARNINGS;
     * +---------+------+-----------------------------------------------+
     * | Level   | Code | Message                                       |
     * +---------+------+-----------------------------------------------+
     * | Warning | 1292 | Truncated incorrect time value: '-1000:57:33' |
     * +---------+------+-----------------------------------------------+
     *
     * @param hour
     * @param minute
     * @param second
     * @return
     */
    public static Time MAKETIME(int hour, int minute, int second) {
        return Time.valueOf(LocalTime.of(hour, minute, second));
    }

    /**
     * MICROSECOND
     * Syntax
     * MICROSECOND(expr)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the microseconds from the time or datetime expression expr as a number in the range from 0 to 999999.
     * <p>
     * If expr is a time with no microseconds, zero is returned, while if expr is a date with no time, zero with a warning is returned.
     * <p>
     * Examples
     * SELECT MICROSECOND('12:00:00.123456');
     * +--------------------------------+
     * | MICROSECOND('12:00:00.123456') |
     * +--------------------------------+
     * |                         123456 |
     * +--------------------------------+
     * <p>
     * SELECT MICROSECOND('2009-12-31 23:59:59.000010');
     * +-------------------------------------------+
     * | MICROSECOND('2009-12-31 23:59:59.000010') |
     * +-------------------------------------------+
     * |                                        10 |
     * +-------------------------------------------+
     * <p>
     * SELECT MICROSECOND('2013-08-07 12:13:14');
     * +------------------------------------+
     * | MICROSECOND('2013-08-07 12:13:14') |
     * +------------------------------------+
     * |                                  0 |
     * +------------------------------------+
     * <p>
     * SELECT MICROSECOND('2013-08-07');
     * +---------------------------+
     * | MICROSECOND('2013-08-07') |
     * +---------------------------+
     * |                         0 |
     * +---------------------------+
     * 1 row in set, 1 warning (0.00 sec)
     * <p>
     * SHOW WARNINGS;
     * +---------+------+----------------------------------------------+
     * | Level   | Code | Message                                      |
     * +---------+------+----------------------------------------------+
     * | Warning | 1292 | Truncated incorrect time value: '2013-08-07' |
     * +---------+------+----------------------------------------------+
     *
     * @param date
     * @return
     */
    public static Long MICROSECOND(Date date) {
        if (date == null) {
            return null;
        }
        long i = LocalDate.ofEpochDay(date.getTime()).get(ChronoField.MICRO_OF_SECOND);
        return i;
    }

    public static Long MICROSECOND(Time date) {
        if (date == null) {
            return null;
        }
        return Long.parseLong(UnsolvedMysqlFunctionUtil.eval("MICROSECOND", date).toString());
    }

    public static Long MICROSECOND(Timestamp date) {
        if (date == null) {
            return null;
        }
        long i = date.toLocalDateTime().get(ChronoField.MICRO_OF_SECOND);
        return i;
    }

    /**
     * MINUTE
     * Syntax
     * MINUTE(time)
     * Description
     * Returns the minute for time, in the range 0 to 59.
     * <p>
     * Examples
     * SELECT MINUTE('2013-08-03 11:04:03');
     * +-------------------------------+
     * | MINUTE('2013-08-03 11:04:03') |
     * +-------------------------------+
     * |                             4 |
     * +-------------------------------+
     * <p>
     * SELECT MINUTE ('23:12:50');
     * +---------------------+
     * | MINUTE ('23:12:50') |
     * +---------------------+
     * |                  12 |
     * +---------------------+
     *
     * @param date
     * @return
     */
    public static Long MINUTES(Timestamp date) {
        if (date == null) {
            return null;
        }
        long i = date.getMinutes();
        return i;
    }

    public static Long MINUTES(Date date) {
        if (date == null) {
            return null;
        }
        long i = date.getMinutes();
        return i;
    }

    public static Long MINUTES(Time date) {
        if (date == null) {
            return null;
        }
        long i = date.getMinutes();
        return i;
    }

    /**
     * MONTH
     * Syntax
     * MONTH(date)
     * Description
     * Returns the month for date in the range 1 to 12 for January to December, or 0 for dates such as '0000-00-00' or '2008-00-00' that have a zero month part.
     * <p>
     * Examples
     * SELECT MONTH('2019-01-03');
     * +---------------------+
     * | MONTH('2019-01-03') |
     * +---------------------+
     * |                   1 |
     * +---------------------+
     * <p>
     * SELECT MONTH('2019-00-03');
     * +---------------------+
     * | MONTH('2019-00-03') |
     * +---------------------+
     * |                   0 |
     * +---------------------+
     *
     * @param date
     * @return
     */
    public static Long MONTH(Time date) {
        if (date == null) {
            return null;
        }
        long i = date.getMonth();
        return i;
    }

    public static Long MONTH(Timestamp date) {
        if (date == null) {
            return null;
        }
        long i = date.getMonth();
        return i;
    }

    public static Long MONTH(Date date) {
        if (date == null) {
            return null;
        }
        long i = date.getMonth();
        return i;
    }

    /**
     * MONTHNAME
     * Syntax
     * MONTHNAME(date)
     * Description
     * Returns the full name of the month for date. The language used for the name is controlled by the value of the lc_time_names system variable. See server locale for more on the supported locales.
     * <p>
     * Examples
     * SELECT MONTHNAME('2019-02-03');
     * +-------------------------+
     * | MONTHNAME('2019-02-03') |
     * +-------------------------+
     * | February                |
     * +-------------------------+
     * Changing the locale:
     * <p>
     * SET lc_time_names = 'fr_CA';
     * <p>
     * SELECT MONTHNAME('2019-05-21');
     * +-------------------------+
     * | MONTHNAME('2019-05-21') |
     * +-------------------------+
     * | mai                     |
     * +-------------------------+
     *
     * @param date
     * @return
     */
    public static String MONTHNAME(Date date) {
        if (date == null) {
            return null;
        }
        return LocalDate.ofEpochDay(date.getTime()).getMonth().getDisplayName(TextStyle.FULL, Locale.ROOT);
    }

    /**
     * NOW
     * Syntax
     * NOW([precision])
     * CURRENT_TIMESTAMP
     * CURRENT_TIMESTAMP([precision])
     * LOCALTIME, LOCALTIME([precision])
     * LOCALTIMESTAMP
     * LOCALTIMESTAMP([precision])
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the current date and time as a value in 'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS.uuuuuu format, depending on whether the function is used in a string or numeric context. The value is expressed in the current time zone.
     * <p>
     * The optional precision determines the microsecond precision. See Microseconds in MariaDB.
     * <p>
     * NOW() (or its synonyms) can be used as the default value for TIMESTAMP columns as well as, since MariaDB 10.0.1, DATETIME columns. Before MariaDB 10.0.1, it was only possible for a single TIMESTAMP column per table to contain the CURRENT_TIMESTAMP as its default.
     * <p>
     * When displayed in the INFORMATION_SCHEMA.COLUMNS table, a default CURRENT TIMESTAMP is displayed as CURRENT_TIMESTAMP up until MariaDB 10.2.2, and as current_timestamp() from MariaDB 10.2.3, due to to MariaDB 10.2 accepting expressions in the DEFAULT clause.
     * <p>
     * Examples
     * SELECT NOW();
     * +---------------------+
     * | NOW()               |
     * +---------------------+
     * | 2010-03-27 13:13:25 |
     * +---------------------+
     * <p>
     * SELECT NOW() + 0;
     * +-----------------------+
     * | NOW() + 0             |
     * +-----------------------+
     * | 20100327131329.000000 |
     * +-----------------------+
     * With precision:
     * <p>
     * SELECT CURRENT_TIMESTAMP(2);
     * +------------------------+
     * | CURRENT_TIMESTAMP(2)   |
     * +------------------------+
     * | 2018-07-10 09:47:26.24 |
     * +------------------------+
     * Used as a default TIMESTAMP:
     * <p>
     * CREATE TABLE t (createdTS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);
     * From MariaDB 10.2.2:
     * <p>
     * SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test'
     * AND COLUMN_NAME LIKE '%ts%'\G
     * *************************** 1. row ***************************
     * TABLE_CATALOG: def
     * TABLE_SCHEMA: test
     * TABLE_NAME: t
     * COLUMN_NAME: ts
     * ORDINAL_POSITION: 1
     * COLUMN_DEFAULT: current_timestamp()
     * ...
     * <= MariaDB 10.2.1
     * <p>
     * SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test'
     * AND COLUMN_NAME LIKE '%ts%'\G
     * *************************** 1. row ***************************
     * TABLE_CATALOG: def
     * TABLE_SCHEMA: test
     * TABLE_NAME: t
     * COLUMN_NAME: createdTS
     * ORDINAL_POSITION: 1
     * COLUMN_DEFAULT: CURRENT_TIMESTAMP
     * ...
     *
     * @param precision
     * @return
     */
    public static Date NOW(int precision) {
        return new Date(LocalDate.parse(UnsolvedMysqlFunctionUtil.eval("now", precision).toString()).toEpochDay());
    }


    /**
     * PERIOD_ADD
     * Syntax
     * PERIOD_ADD(P,N)
     * Description
     * Adds N months to period P. P is in the format YYMM or YYYYMM, and is not a date value. If P contains a two-digit year, values from 00 to 69 are converted to from 2000 to 2069, while values from 70 are converted to 1970 upwards.
     * <p>
     * Returns a value in the format YYYYMM.
     * <p>
     * Examples
     * SELECT PERIOD_ADD(200801,2);
     * +----------------------+
     * | PERIOD_ADD(200801,2) |
     * +----------------------+
     * |               200803 |
     * +----------------------+
     * <p>
     * SELECT PERIOD_ADD(6910,2);
     * +--------------------+
     * | PERIOD_ADD(6910,2) |
     * +--------------------+
     * |             206912 |
     * +--------------------+
     * <p>
     * SELECT PERIOD_ADD(7010,2);
     * +--------------------+
     * | PERIOD_ADD(7010,2) |
     * +--------------------+
     * |             197012 |
     * +--------------------+
     *
     * @param P
     * @param N
     * @return
     */
    public static Integer PERIOD_ADD(Integer P, Integer N) {
        return Integer.parseInt(UnsolvedMysqlFunctionUtil.eval("PERIOD_ADD", P, N).toString());
    }

    /**
     * Syntax
     * PERIOD_DIFF(P1,P2)
     * Description
     * Returns the number of months between periods P1 and P2. P1 and P2 can be in the format YYMM or YYYYMM, and are not date values.
     * <p>
     * If P1 or P2 contains a two-digit year, values from 00 to 69 are converted to from 2000 to 2069, while values from 70 are converted to 1970 upwards.
     * <p>
     * Examples
     * SELECT PERIOD_DIFF(200802,200703);
     * +----------------------------+
     * | PERIOD_DIFF(200802,200703) |
     * +----------------------------+
     * |                         11 |
     * +----------------------------+
     * <p>
     * SELECT PERIOD_DIFF(6902,6803);
     * +------------------------+
     * | PERIOD_DIFF(6902,6803) |
     * +------------------------+
     * |                     11 |
     * +------------------------+
     * <p>
     * SELECT PERIOD_DIFF(7002,6803);
     * +------------------------+
     * | PERIOD_DIFF(7002,6803) |
     * +------------------------+
     * |                  -1177 |
     * +------------------------+
     *
     * @param P
     * @param N
     * @return
     */
    public static Integer PERIOD_DIFF(Integer P, Integer N) {
        return Integer.parseInt(UnsolvedMysqlFunctionUtil.eval("PERIOD_DIFF", P, N).toString());
    }

    /**
     * QUARTER
     * Syntax
     * QUARTER(date)
     * Description
     * Returns the quarter of the year for date, in the range 1 to 4. Returns 0 if month contains a zero value, or NULL if the given value is not otherwise a valid date (zero values are accepted).
     * <p>
     * Examples
     * SELECT QUARTER('2008-04-01');
     * +-----------------------+
     * | QUARTER('2008-04-01') |
     * +-----------------------+
     * |                     2 |
     * +-----------------------+
     * <p>
     * SELECT QUARTER('2019-00-01');
     * +-----------------------+
     * | QUARTER('2019-00-01') |
     * +-----------------------+
     * |                     0 |
     * +-----------------------+
     *
     * @param date
     * @return
     */
    public static Integer QUARTER(Date date) {
        return Integer.parseInt(UnsolvedMysqlFunctionUtil.eval("QUARTER", date).toString());
    }

    /**
     * SECOND
     * Syntax
     * SECOND(time)
     * Description
     * Returns the second for a given time (which can include microseconds), in the range 0 to 59, or NULL if not given a valid time value.
     * <p>
     * Examples
     * SELECT SECOND('10:05:03');
     * +--------------------+
     * | SECOND('10:05:03') |
     * +--------------------+
     * |                  3 |
     * +--------------------+
     * <p>
     * SELECT SECOND('10:05:01.999999');
     * +---------------------------+
     * | SECOND('10:05:01.999999') |
     * +---------------------------+
     * |                         1 |
     * +---------------------------+
     *
     * @param time
     * @return
     */
    public static Integer SECOND(Timestamp time) {
        if (time == null) {
            return null;
        }
        return time.toLocalDateTime().getSecond();
    }

    public static Integer SECOND(Time time) {
        if (time == null) {
            return null;
        }
        return time.getSeconds();
    }

    public static Integer SECOND(Date time) {
        if (time == null) {
            return null;
        }
        return time.getSeconds();
    }


    /**
     * SEC_TO_TIME
     * Syntax
     * SEC_TO_TIME(seconds)
     * Description
     * Returns the seconds argument, converted to hours, minutes, and seconds, as a TIME value. The range of the result is constrained to that of the TIME data type. A warning occurs if the argument corresponds to a value outside that range.
     * <p>
     * The time will be returned in the format hh:mm:ss, or hhmmss if used in a numeric calculation.
     * <p>
     * Examples
     * SELECT SEC_TO_TIME(12414);
     * +--------------------+
     * | SEC_TO_TIME(12414) |
     * +--------------------+
     * | 03:26:54           |
     * +--------------------+
     * <p>
     * SELECT SEC_TO_TIME(12414)+0;
     * +----------------------+
     * | SEC_TO_TIME(12414)+0 |
     * +----------------------+
     * |                32654 |
     * +----------------------+
     * <p>
     * SELECT SEC_TO_TIME(9999999);
     * +----------------------+
     * | SEC_TO_TIME(9999999) |
     * +----------------------+
     * | 838:59:59            |
     * +----------------------+
     * 1 row in set, 1 warning (0.00 sec)
     * <p>
     * SHOW WARNINGS;
     * +---------+------+-------------------------------------------+
     * | Level   | Code | Message                                   |
     * +---------+------+-------------------------------------------+
     * | Warning | 1292 | Truncated incorrect time value: '9999999' |
     * +---------+------+-------------------------------------------+
     *
     * @param second
     * @return
     */
    public static Timestamp SEC_TO_TIME(Long second) {
        if (second == null) {
            return null;
        }
        return Timestamp.from(Instant.ofEpochSecond(second));
    }


    /**
     * STR_TO_DATE
     * Syntax
     * STR_TO_DATE(str,format)
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * This is the inverse of the DATE_FORMAT() function. It takes a string str and a format string format. STR_TO_DATE() returns a DATETIME value if the format string contains both date and time parts, or a DATE or TIME value if the string contains only date or time parts.
     * <p>
     * The date, time, or datetime values contained in str should be given in the format indicated by format. If str contains an illegal date, time, or datetime value, STR_TO_DATE() returns NULL. An illegal value also produces a warning.
     * <p>
     * The options that can be used by STR_TO_DATE(), as well as its inverse DATE_FORMAT() and the FROM_UNIXTIME() function, are:
     * <p>
     * Option	Description
     * %a	Short weekday name in current locale (Variable lc_time_names).
     * %b	Short form month name in current locale. For locale en_US this is one of: Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov or Dec.
     * %c	Month with 1 or 2 digits.
     * %D	Day with English suffix 'th', 'nd', 'st' or 'rd''. (1st, 2nd, 3rd...).
     * %d	Day with 2 digits.
     * %e	Day with 1 or 2 digits.
     * %f	Sub seconds 6 digits.
     * %H	Hour with 2 digits between 00-23.
     * %h	Hour with 2 digits between 01-12.
     * %I	Hour with 2 digits between 01-12.
     * %i	Minute with 2 digits.
     * %j	Day of the year (001-366)
     * %k	Hour with 1 digits between 0-23.
     * %l	Hour with 1 digits between 1-12.
     * %M	Full month name in current locale (Variable lc_time_names).
     * %m	Month with 2 digits.
     * %p	AM/PM according to current locale (Variable lc_time_names).
     * %r	Time in 12 hour format, followed by AM/PM. Short for '%I:%i:%S %p'.
     * %S	Seconds with 2 digits.
     * %s	Seconds with 2 digits.
     * %T	Time in 24 hour format. Short for '%H:%i:%S'.
     * %U	Week number (00-53), when first day of the week is Sunday.
     * %u	Week number (00-53), when first day of the week is Monday.
     * %V	Week number (01-53), when first day of the week is Sunday. Used with %X.
     * %v	Week number (01-53), when first day of the week is Monday. Used with %x.
     * %W	Full weekday name in current locale (Variable lc_time_names).
     * %w	Day of the week. 0 = Sunday, 6 = Saturday.
     * %X	Year with 4 digits when first day of the week is Sunday. Used with %V.
     * %x	Year with 4 digits when first day of the week is Monday. Used with %v.
     * %Y	Year with 4 digits.
     * %y	Year with 2 digits.
     * %#	For str_to_date(), skip all numbers.
     * %.	For str_to_date(), skip all punctation characters.
     * %@	For str_to_date(), skip all alpha characters.
     * %%	A literal % character.
     * Examples
     * SELECT STR_TO_DATE('Wednesday, June 2, 2014', '%W, %M %e, %Y');
     * +---------------------------------------------------------+
     * | STR_TO_DATE('Wednesday, June 2, 2014', '%W, %M %e, %Y') |
     * +---------------------------------------------------------+
     * | 2014-06-02                                              |
     * +---------------------------------------------------------+
     * <p>
     * <p>
     * SELECT STR_TO_DATE('Wednesday23423, June 2, 2014', '%W, %M %e, %Y');
     * +--------------------------------------------------------------+
     * | STR_TO_DATE('Wednesday23423, June 2, 2014', '%W, %M %e, %Y') |
     * +--------------------------------------------------------------+
     * | NULL                                                         |
     * +--------------------------------------------------------------+
     * 1 row in set, 1 warning (0.00 sec)
     * <p>
     * SHOW WARNINGS;
     * +---------+------+-----------------------------------------------------------------------------------+
     * | Level   | Code | Message                                                                           |
     * +---------+------+-----------------------------------------------------------------------------------+
     * | Warning | 1411 | Incorrect datetime value: 'Wednesday23423, June 2, 2014' for function str_to_date |
     * +---------+------+-----------------------------------------------------------------------------------+
     * <p>
     * SELECT STR_TO_DATE('Wednesday23423, June 2, 2014', '%W%#, %M %e, %Y');
     * +----------------------------------------------------------------+
     * | STR_TO_DATE('Wednesday23423, June 2, 2014', '%W%#, %M %e, %Y') |
     * +----------------------------------------------------------------+
     * | 2014-06-02                                                     |
     * +----------------------------------------------------------------+
     * See Also
     * DATE_FORMAT()
     * FROM_UNIXTIME()
     *
     * @param str
     * @param format
     * @return
     */
    public static Date STR_TO_DATE(String str, String format) {
        if (str == null || format == null) {
            return null;
        }
        return new Date(UnsolvedMysqlFunctionUtil.eval("STR_TO_DATE", str, format).toString());
    }

    /**
     * SUBDATE
     * Syntax
     * SUBDATE(date,INTERVAL expr unit), SUBDATE(expr,days)
     * Description
     * When invoked with the INTERVAL form of the second argument, SUBDATE() is a synonym for DATE_SUB(). See Date and Time Units for a complete list of permitted units.
     * <p>
     * The second form allows the use of an integer value for days. In such cases, it is interpreted as the number of days to be subtracted from the date or datetime expression expr.
     * <p>
     * Examples
     * SELECT DATE_SUB('2008-01-02', INTERVAL 31 DAY);
     * +-----------------------------------------+
     * | DATE_SUB('2008-01-02', INTERVAL 31 DAY) |
     * +-----------------------------------------+
     * | 2007-12-02                              |
     * +-----------------------------------------+
     * <p>
     * SELECT SUBDATE('2008-01-02', INTERVAL 31 DAY);
     * +----------------------------------------+
     * | SUBDATE('2008-01-02', INTERVAL 31 DAY) |
     * +----------------------------------------+
     * | 2007-12-02                             |
     * +----------------------------------------+
     * SELECT SUBDATE('2008-01-02 12:00:00', 31);
     * +------------------------------------+
     * | SUBDATE('2008-01-02 12:00:00', 31) |
     * +------------------------------------+
     * | 2007-12-02 12:00:00                |
     * +------------------------------------+
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * SELECT d, SUBDATE(d, 10) from t1;
     * +---------------------+---------------------+
     * | d                   | SUBDATE(d, 10)      |
     * +---------------------+---------------------+
     * | 2007-01-30 21:31:07 | 2007-01-20 21:31:07 |
     * | 1983-10-15 06:42:51 | 1983-10-05 06:42:51 |
     * | 2011-04-21 12:34:56 | 2011-04-11 12:34:56 |
     * | 2011-10-30 06:31:41 | 2011-10-20 06:31:41 |
     * | 2011-01-30 14:03:25 | 2011-01-20 14:03:25 |
     * | 2004-10-07 11:19:34 | 2004-09-27 11:19:34 |
     * +---------------------+---------------------+
     * <p>
     * SELECT d, SUBDATE(d, INTERVAL 10 MINUTE) from t1;
     * +---------------------+--------------------------------+
     * | d                   | SUBDATE(d, INTERVAL 10 MINUTE) |
     * +---------------------+--------------------------------+
     * | 2007-01-30 21:31:07 | 2007-01-30 21:21:07            |
     * | 1983-10-15 06:42:51 | 1983-10-15 06:32:51            |
     * | 2011-04-21 12:34:56 | 2011-04-21 12:24:56            |
     * | 2011-10-30 06:31:41 | 2011-10-30 06:21:41            |
     * | 2011-01-30 14:03:25 | 2011-01-30 13:53:25            |
     * | 2004-10-07 11:19:34 | 2004-10-07 11:09:34            |
     * +---------------------+--------------------------------+
     *
     * @param date
     * @param time
     * @return
     */
    public static Date SUBDATE(Date date, Timestamp time) {
        if (date == null || time == null) {
            return null;
        }
        return new Date(UnsolvedMysqlFunctionUtil.eval("SUBDATE", date, time).toString());
    }

    public static Date SUBDATE(Date date, Time time) {
        if (date == null || time == null) {
            return null;
        }
        return new Date(UnsolvedMysqlFunctionUtil.eval("SUBDATE", date, time).toString());
    }

    public static Date SUBTIME(Date date, Time time) {
        if (date == null || time == null) {
            return null;
        }
        return new Date(UnsolvedMysqlFunctionUtil.eval("SUBTIME", date, time).toString());
    }

    public static Time SUBTIME(Time date, Time time) {
        if (date == null || time == null) {
            return null;
        }
        return new Time(date.getTime() - time.getTime());
    }

    /**
     * SYSDATE
     * Syntax
     * SYSDATE([precision])
     * Contents
     * Syntax
     * Description
     * Examples
     * See Also
     * Description
     * Returns the current date and time as a value in 'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS.uuuuuu format, depending on whether the function is used in a string or numeric context.
     * <p>
     * The optional precision determines the microsecond precision. See Microseconds in MariaDB.
     * <p>
     * SYSDATE() returns the time at which it executes. This differs from the behavior for NOW(), which returns a constant time that indicates the time at which the statement began to execute. (Within a stored routine or trigger, NOW() returns the time at which the routine or triggering statement began to execute.)
     * <p>
     * In addition, changing the timestamp system variable with a SET timestamp statement affects the value returned by NOW() but not by SYSDATE(). This means that timestamp settings in the binary log have no effect on invocations of SYSDATE().
     * <p>
     * Because SYSDATE() can return different values even within the same statement, and is not affected by SET TIMESTAMP, it is non-deterministic and therefore unsafe for replication if statement-based binary logging is used. If that is a problem, you can use row-based logging, or start the server with the mysqld option --sysdate-is-now to cause SYSDATE() to be an alias for NOW(). The non-deterministic nature of SYSDATE() also means that indexes cannot be used for evaluating expressions that refer to it, and that statements using the SYSDATE() function are unsafe for statement-based replication.
     * <p>
     * Examples
     * Difference between NOW() and SYSDATE():
     * <p>
     * SELECT NOW(), SLEEP(2), NOW();
     * +---------------------+----------+---------------------+
     * | NOW()               | SLEEP(2) | NOW()               |
     * +---------------------+----------+---------------------+
     * | 2010-03-27 13:23:40 |        0 | 2010-03-27 13:23:40 |
     * +---------------------+----------+---------------------+
     * <p>
     * SELECT SYSDATE(), SLEEP(2), SYSDATE();
     * +---------------------+----------+---------------------+
     * | SYSDATE()           | SLEEP(2) | SYSDATE()           |
     * +---------------------+----------+---------------------+
     * | 2010-03-27 13:23:52 |        0 | 2010-03-27 13:23:54 |
     * +---------------------+----------+---------------------+
     * With precision:
     * <p>
     * SELECT SYSDATE(4);
     * +--------------------------+
     * | SYSDATE(4)               |
     * +--------------------------+
     * | 2018-07-10 10:17:13.1689 |
     * +--------------------------+
     *
     * @return
     */
    public static Date SYSDATE() {
        return new Date(UnsolvedMysqlFunctionUtil.eval("SYSDATE").toString());
    }

    public static Date SYSDATE(int precision) {
        return new Date(UnsolvedMysqlFunctionUtil.eval("SYSDATE", precision).toString());
    }

    /**
     * TIME Function
     * Syntax
     * TIME(expr)
     * Description
     * Extracts the time part of the time or datetime expression expr and returns it as a string.
     * <p>
     * Examples
     * SELECT TIME('2003-12-31 01:02:03');
     * +-----------------------------+
     * | TIME('2003-12-31 01:02:03') |
     * +-----------------------------+
     * | 01:02:03                    |
     * +-----------------------------+
     * <p>
     * SELECT TIME('2003-12-31 01:02:03.000123');
     * +------------------------------------+
     * | TIME('2003-12-31 01:02:03.000123') |
     * +------------------------------------+
     * | 01:02:03.000123                    |
     * +------------------------------------+
     *
     * @param date
     * @return
     */
    public static Timestamp time(Date date) {
        return new Timestamp(Long.parseLong(UnsolvedMysqlFunctionUtil.eval("time", date).toString()));
    }

    /**
     * TIMEDIFF
     * Syntax
     * TIMEDIFF(expr1,expr2)
     * Description
     * TIMEDIFF() returns expr1 - expr2 expressed as a time value. expr1 and expr2 are time or date-and-time expressions, but both must be of the same type.
     * <p>
     * Examples
     * SELECT TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001');
     * +---------------------------------------------------------------+
     * | TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001') |
     * +---------------------------------------------------------------+
     * | -00:00:00.000001                                              |
     * +---------------------------------------------------------------+
     * <p>
     * SELECT TIMEDIFF('2008-12-31 23:59:59.000001', '2008-12-30 01:01:01.000002');
     * +----------------------------------------------------------------------+
     * | TIMEDIFF('2008-12-31 23:59:59.000001', '2008-12-30 01:01:01.000002') |
     * +----------------------------------------------------------------------+
     * | 46:58:57.999999                                                      |
     * +----------------------------------------------------------------------+
     *
     * @param arg0
     * @param arg1
     * @return
     */
    public static Timestamp timeDiff(Timestamp arg0, Timestamp arg1) {
        return new Timestamp(arg0.getTime() - arg1.getTime());
    }


    /**
     * TIMESTAMP FUNCTION
     * Syntax
     * TIMESTAMP(expr), TIMESTAMP(expr1,expr2)
     * Description
     * With a single argument, this function returns the date or datetime expression expr as a datetime value. With two arguments, it adds the time expression expr2 to the date or datetime expression expr1 and returns the result as a datetime value.
     * <p>
     * Examples
     * SELECT TIMESTAMP('2003-12-31');
     * +-------------------------+
     * | TIMESTAMP('2003-12-31') |
     * +-------------------------+
     * | 2003-12-31 00:00:00     |
     * +-------------------------+
     * <p>
     * SELECT TIMESTAMP('2003-12-31 12:00:00','6:30:00');
     * +--------------------------------------------+
     * | TIMESTAMP('2003-12-31 12:00:00','6:30:00') |
     * +--------------------------------------------+
     * | 2003-12-31 18:30:00                        |
     * +--------------------------------------------+
     *
     * @param arg0
     * @return
     */
    public static Timestamp TIMESTAMP(Object arg0) {
        return new Timestamp(Long.parseLong(UnsolvedMysqlFunctionUtil.eval("TIMESTAMP", arg0).toString()));
    }

    public static Timestamp TIMESTAMP(Object arg0, Object arg1) {
        return new Timestamp(Long.parseLong(UnsolvedMysqlFunctionUtil.eval("TIMESTAMP", arg0, arg1).toString()));
    }

    /**
     * TIMESTAMPADD
     * Syntax
     * TIMESTAMPADD(unit,interval,datetime_expr)
     * Description
     * Adds the integer expression interval to the date or datetime expression datetime_expr. The unit for interval is given by the unit argument, which should be one of the following values: MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.
     * <p>
     * The unit value may be specified using one of keywords as shown, or with a prefix of SQL_TSI_. For example, DAY and SQL_TSI_DAY both are legal.
     * <p>
     * Before MariaDB 5.5, FRAC_SECOND was permitted as a synonym for MICROSECOND.
     * <p>
     * Examples
     * SELECT TIMESTAMPADD(MINUTE,1,'2003-01-02');
     * +-------------------------------------+
     * | TIMESTAMPADD(MINUTE,1,'2003-01-02') |
     * +-------------------------------------+
     * | 2003-01-02 00:01:00                 |
     * +-------------------------------------+
     * <p>
     * SELECT TIMESTAMPADD(WEEK,1,'2003-01-02');
     * +-----------------------------------+
     * | TIMESTAMPADD(WEEK,1,'2003-01-02') |
     * +-----------------------------------+
     * | 2003-01-09                        |
     * +-----------------------------------+
     *
     * @param arg0
     * @param arg1
     * @param arg2
     * @return
     */
    public static Timestamp TIMESTAMPADD(Object arg0, Object arg1, Object arg2) {
        return new Timestamp(Long.parseLong(UnsolvedMysqlFunctionUtil.eval("TIMESTAMPADD", arg0, arg1, arg2).toString()));
    }

    /**
     * TIMESTAMPDIFF
     * Syntax
     * TIMESTAMPDIFF(unit,datetime_expr1,datetime_expr2)
     * Description
     * Returns datetime_expr2 - datetime_expr1, where datetime_expr1 and datetime_expr2 are date or datetime expressions. One expression may be a date and the other a datetime; a date value is treated as a datetime having the time part '00:00:00' where necessary. The unit for the result (an integer) is given by the unit argument. The legal values for unit are the same as those listed in the description of the TIMESTAMPADD() function, i.e MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.
     * <p>
     * TIMESTAMPDIFF can also be used to calculate age.
     * <p>
     * Examples
     * SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01');
     * +------------------------------------------------+
     * | TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01') |
     * +------------------------------------------------+
     * |                                              3 |
     * +------------------------------------------------+
     * <p>
     * SELECT TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01');
     * +-----------------------------------------------+
     * | TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01') |
     * +-----------------------------------------------+
     * |                                            -1 |
     * +-----------------------------------------------+
     * <p>
     * SELECT TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55');
     * +----------------------------------------------------------+
     * | TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55') |
     * +----------------------------------------------------------+
     * |                                                   128885 |
     * +----------------------------------------------------------+
     * Calculating age:
     * <p>
     * SELECT CURDATE();
     * +------------+
     * | CURDATE()  |
     * +------------+
     * | 2019-05-27 |
     * +------------+
     * <p>
     * SELECT TIMESTAMPDIFF(YEAR, '1971-06-06', CURDATE()) AS age;
     * +------+
     * | age  |
     * +------+
     * |   47 |
     * +------+
     * <p>
     * SELECT TIMESTAMPDIFF(YEAR, '1971-05-06', CURDATE()) AS age;
     * +------+
     * | age  |
     * +------+
     * |   48 |
     * +------+
     * Age as of 2014-08-02:
     * <p>
     * SELECT name, date_of_birth, TIMESTAMPDIFF(YEAR,date_of_birth,'2014-08-02') AS age
     * FROM student_details;
     * +---------+---------------+------+
     * | name    | date_of_birth | age  |
     * +---------+---------------+------+
     * | Chun    | 1993-12-31    |   20 |
     * | Esben   | 1946-01-01    |   68 |
     * | Kaolin  | 1996-07-16    |   18 |
     * | Tatiana | 1988-04-13    |   26 |
     * +---------+---------------+------+
     *
     * @param arg0
     * @param arg1
     * @param arg2
     * @return
     */
    public static Timestamp TIMESTAMPDIFF(Object arg0, Object arg1, Object arg2) {
        return new Timestamp(Long.parseLong(UnsolvedMysqlFunctionUtil.eval("TIMESTAMPDIFF", arg0, arg1).toString()));
    }

    /**
     * TIME_FORMAT
     * Syntax
     * TIME_FORMAT(time,format)
     * Description
     * This is used like the DATE_FORMAT() function, but the format string may contain format specifiers only for hours, minutes, and seconds. Other specifiers produce a NULL value or 0.
     * <p>
     * Examples
     * SELECT TIME_FORMAT('100:00:00', '%H %k %h %I %l');
     * +--------------------------------------------+
     * | TIME_FORMAT('100:00:00', '%H %k %h %I %l') |
     * +--------------------------------------------+
     * | 100 100 04 04 4                            |
     * +--------------------------------------------+
     *
     * @param arg0
     * @param arg1
     * @return
     */
    public static String TIME_FORMAT(Object arg0, Object arg1) {
        return ((UnsolvedMysqlFunctionUtil.eval("TIME_FORMAT", arg0, arg1).toString()));
    }

    /**
     * TIME_TO_SEC
     * Syntax
     * TIME_TO_SEC(time)
     * Description
     * Returns the time argument, converted to seconds.
     * <p>
     * The value returned by TIME_TO_SEC is of type DOUBLE. Before MariaDB 5.3 (and MySQL 5.6), the type was INT. See Microseconds in MariaDB.
     * <p>
     * Examples
     * SELECT TIME_TO_SEC('22:23:00');
     * +-------------------------+
     * | TIME_TO_SEC('22:23:00') |
     * +-------------------------+
     * |                   80580 |
     * +-------------------------+
     * SELECT TIME_TO_SEC('00:39:38');
     * +-------------------------+
     * | TIME_TO_SEC('00:39:38') |
     * +-------------------------+
     * |                    2378 |
     * +-------------------------+
     *
     * @param arg0
     * @return
     */
    public static Double TIME_TO_SEC(Object arg0) {
        return Double.parseDouble((UnsolvedMysqlFunctionUtil.eval("TIME_TO_SEC", arg0).toString()));
    }


    /**
     * TO_DAYS
     * Syntax
     * TO_DAYS(date)
     * Description
     * Given a date date, returns the number of days since the start of the current calendar (0000-00-00).
     * <p>
     * The function is not designed for use with dates before the advent of the Gregorian calendar in October 1582. Results will not be reliable since it doesn't account for the lost days when the calendar changed from the Julian calendar.
     * <p>
     * This is the converse of the FROM_DAYS() function.
     * <p>
     * Examples
     * SELECT TO_DAYS('2007-10-07');
     * +-----------------------+
     * | TO_DAYS('2007-10-07') |
     * +-----------------------+
     * |                733321 |
     * +-----------------------+
     * <p>
     * SELECT TO_DAYS('0000-01-01');
     * +-----------------------+
     * | TO_DAYS('0000-01-01') |
     * +-----------------------+
     * |                     1 |
     * +-----------------------+
     * <p>
     * SELECT TO_DAYS(950501);
     * +-----------------+
     * | TO_DAYS(950501) |
     * +-----------------+
     * |          728779 |
     * +-----------------+
     *
     * @param arg0
     * @return
     */
    public static Long TO_DAYS(Object arg0) {
        return Long.parseLong((UnsolvedMysqlFunctionUtil.eval("TO_DAYS", arg0).toString()));
    }

    /**
     * TO_SECONDS
     * Syntax
     * TO_SECONDS(expr)
     * Description
     * Returns the number of seconds from year 0 till expr, or NULL if expr is not a valid date or datetime.
     * <p>
     * Examples
     * SELECT TO_SECONDS('2013-06-13');
     * +--------------------------+
     * | TO_SECONDS('2013-06-13') |
     * +--------------------------+
     * |              63538300800 |
     * +--------------------------+
     * <p>
     * SELECT TO_SECONDS('2013-06-13 21:45:13');
     * +-----------------------------------+
     * | TO_SECONDS('2013-06-13 21:45:13') |
     * +-----------------------------------+
     * |                       63538379113 |
     * +-----------------------------------+
     * <p>
     * SELECT TO_SECONDS(NOW());
     * +-------------------+
     * | TO_SECONDS(NOW()) |
     * +-------------------+
     * |       63543530875 |
     * +-------------------+
     * <p>
     * SELECT TO_SECONDS(20130513);
     * +----------------------+
     * | TO_SECONDS(20130513) |
     * +----------------------+
     * |          63535622400 |
     * +----------------------+
     * 1 row in set (0.00 sec)
     * <p>
     * SELECT TO_SECONDS(130513);
     * +--------------------+
     * | TO_SECONDS(130513) |
     * +--------------------+
     * |        63535622400 |
     * +--------------------+
     *
     * @param arg0
     * @return
     */
    public static Long TO_SECONDS(Object arg0) {
        return Long.parseLong((UnsolvedMysqlFunctionUtil.eval("TO_SECONDS", arg0).toString()));
    }

    /**
     * UNIX_TIMESTAMP
     * Contents
     * Syntax
     * Description
     * Error Handling
     * Compatibility
     * Examples
     * See Also
     * Syntax
     * UNIX_TIMESTAMP()
     * UNIX_TIMESTAMP(date)
     * Description
     * If called with no argument, returns a Unix timestamp (seconds since '1970-01-01 00:00:00' UTC) as an unsigned integer. If UNIX_TIMESTAMP() is called with a date argument, it returns the value of the argument as seconds since '1970-01-01 00:00:00' UTC. date may be a DATE string, a DATETIME string, a TIMESTAMP, or a number in the format YYMMDD or YYYYMMDD. The server interprets date as a value in the current time zone and converts it to an internal value in UTC. Clients can set their time zone as described in time zones.
     * <p>
     * The inverse function of UNIX_TIMESTAMP() is FROM_UNIXTIME()
     * <p>
     * UNIX_TIMESTAMP() supports microseconds.
     * <p>
     * Timestamps in MariaDB have a maximum value of 2147483647, equivalent to 2038-01-19 05:14:07. This is due to the underlying 32-bit limitation. Using the function on a date beyond this will result in NULL being returned. Use DATETIME as a storage type if you require dates beyond this.
     * <p>
     * Error Handling
     * Returns NULL for wrong arguments to UNIX_TIMESTAMP(). In MySQL and MariaDB before 5.3 wrong arguments to UNIX_TIMESTAMP() returned 0.
     * <p>
     * Compatibility
     * As you can see in the examples above, UNIX_TIMESTAMP(constant-date-string) returns a timestamp with 6 decimals while MariaDB 5.2 and before returns it without decimals. This can cause a problem if you are using UNIX_TIMESTAMP() as a partitioning function. You can fix this by using FLOOR(UNIX_TIMESTAMP(..)) or changing the date string to a date number, like 20080101000000.
     * <p>
     * Examples
     * SELECT UNIX_TIMESTAMP();
     * +------------------+
     * | UNIX_TIMESTAMP() |
     * +------------------+
     * |       1269711082 |
     * +------------------+
     * <p>
     * SELECT UNIX_TIMESTAMP('2007-11-30 10:30:19');
     * +---------------------------------------+
     * | UNIX_TIMESTAMP('2007-11-30 10:30:19') |
     * +---------------------------------------+
     * |                     1196436619.000000 |
     * +---------------------------------------+
     * <p>
     * SELECT UNIX_TIMESTAMP("2007-11-30 10:30:19.123456");
     * +----------------------------------------------+
     * | unix_timestamp("2007-11-30 10:30:19.123456") |
     * +----------------------------------------------+
     * |                            1196411419.123456 |
     * +----------------------------------------------+
     * <p>
     * SELECT FROM_UNIXTIME(UNIX_TIMESTAMP('2007-11-30 10:30:19'));
     * +------------------------------------------------------+
     * | FROM_UNIXTIME(UNIX_TIMESTAMP('2007-11-30 10:30:19')) |
     * +------------------------------------------------------+
     * | 2007-11-30 10:30:19.000000                           |
     * +------------------------------------------------------+
     * <p>
     * SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP('2007-11-30 10:30:19')));
     * +-------------------------------------------------------------+
     * | FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP('2007-11-30 10:30:19'))) |
     * +-------------------------------------------------------------+
     * | 2007-11-30 10:30:19                                         |
     * +-------------------------------------------------------------+
     *
     * @return
     */
    public static Long UNIX_TIMESTAMP() {
        return Long.parseLong((UnsolvedMysqlFunctionUtil.eval("UNIX_TIMESTAMP").toString()));
    }

    public static Long UNIX_TIMESTAMP(Object arg) {
        return Long.parseLong((UnsolvedMysqlFunctionUtil.eval("UNIX_TIMESTAMP", arg).toString()));
    }


    /**
     * UTC_DATE
     * Syntax
     * UTC_DATE, UTC_DATE()
     * Description
     * Returns the current UTC date as a value in 'YYYY-MM-DD' or YYYYMMDD format, depending on whether the function is used in a string or numeric context.
     * <p>
     * Examples
     * SELECT UTC_DATE(), UTC_DATE() + 0;
     * +------------+----------------+
     * | UTC_DATE() | UTC_DATE() + 0 |
     * +------------+----------------+
     * | 2010-03-27 |       20100327 |
     * +------------+----------------+
     *
     * @param arg
     * @return
     */
    public static Long UTC_DATE(Object arg) {
        return Long.parseLong((UnsolvedMysqlFunctionUtil.eval("UTC_DATE", arg).toString()));
    }

    /**
     * UTC_TIME
     * Syntax
     * UTC_TIME
     * UTC_TIME([precision])
     * Description
     * Returns the current UTC time as a value in 'HH:MM:SS' or HHMMSS.uuuuuu format, depending on whether the function is used in a string or numeric context.
     * <p>
     * The optional precision determines the microsecond precision. See Microseconds in MariaDB.
     * <p>
     * Examples
     * SELECT UTC_TIME(), UTC_TIME() + 0;
     * +------------+----------------+
     * | UTC_TIME() | UTC_TIME() + 0 |
     * +------------+----------------+
     * | 17:32:34   |  173234.000000 |
     * +------------+----------------+
     * With precision:
     * <p>
     * SELECT UTC_TIME(5);
     * +----------------+
     * | UTC_TIME(5)    |
     * +----------------+
     * | 07:52:50.78369 |
     * +----------------+
     *
     * @return
     */
    public static Long UTC_TIME() {
        return Long.parseLong((UnsolvedMysqlFunctionUtil.eval("UTC_TIME").toString()));
    }

    public static Long UTC_TIME(Object arg) {
        return Long.parseLong((UnsolvedMysqlFunctionUtil.eval("UTC_TIME", arg).toString()));
    }

    /**
     * UTC_TIMESTAMP
     * Syntax
     * UTC_TIMESTAMP
     * UTC_TIMESTAMP([precision])
     * Description
     * Returns the current UTC date and time as a value in 'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS.uuuuuu format, depending on whether the function is used in a string or numeric context.
     * <p>
     * The optional precision determines the microsecond precision. See Microseconds in MariaDB.
     * <p>
     * Examples
     * SELECT UTC_TIMESTAMP(), UTC_TIMESTAMP() + 0;
     * +---------------------+-----------------------+
     * | UTC_TIMESTAMP()     | UTC_TIMESTAMP() + 0   |
     * +---------------------+-----------------------+
     * | 2010-03-27 17:33:16 | 20100327173316.000000 |
     * +---------------------+-----------------------+
     * With precision:
     * <p>
     * SELECT UTC_TIMESTAMP(4);
     * +--------------------------+
     * | UTC_TIMESTAMP(4)         |
     * +--------------------------+
     * | 2018-07-10 07:51:09.1019 |
     * +--------------------------+
     * See Also
     *
     * @param arg
     * @return
     */
    public static Long UTC_TIMESTAMP(Object arg) {
        return Long.parseLong((UnsolvedMysqlFunctionUtil.eval("UTC_TIMESTAMP", arg).toString()));
    }

    public static Long UTC_TIMESTAMP() {
        return Long.parseLong((UnsolvedMysqlFunctionUtil.eval("UTC_TIMESTAMP").toString()));
    }

    /**
     * WEEK
     * Syntax
     * WEEK(date[,mode])
     * Description
     * This function returns the week number for date. The two-argument form of WEEK() allows you to specify whether the week starts on Sunday or Monday and whether the return value should be in the range from 0 to 53 or from 1 to 53. If the mode argument is omitted, the value of the default_week_format system variable is used.
     * <p>
     * Modes
     * Mode	1st day of week	Range	Week 1 is the 1st week with
     * 0	Sunday	0-53	a Sunday in this year
     * 1	Monday	0-53	more than 3 days this year
     * 2	Sunday	1-53	a Sunday in this year
     * 3	Monday	1-53	more than 3 days this year
     * 4	Sunday	0-53	more than 3 days this year
     * 5	Monday	0-53	a Monday in this year
     * 6	Sunday	1-53	more than 3 days this year
     * 7	Monday	1-53	a Monday in this year
     * With the mode value of 3, which means “more than 3 days this year”, weeks are numbered according to ISO 8601:1988.
     * <p>
     * Examples
     * SELECT WEEK('2008-02-20');
     * +--------------------+
     * | WEEK('2008-02-20') |
     * +--------------------+
     * |                  7 |
     * +--------------------+
     * <p>
     * SELECT WEEK('2008-02-20',0);
     * +----------------------+
     * | WEEK('2008-02-20',0) |
     * +----------------------+
     * |                    7 |
     * +----------------------+
     * <p>
     * SELECT WEEK('2008-02-20',1);
     * +----------------------+
     * | WEEK('2008-02-20',1) |
     * +----------------------+
     * |                    8 |
     * +----------------------+
     * <p>
     * SELECT WEEK('2008-12-31',0);
     * +----------------------+
     * | WEEK('2008-12-31',0) |
     * +----------------------+
     * |                   52 |
     * +----------------------+
     * <p>
     * SELECT WEEK('2008-12-31',1);
     * +----------------------+
     * | WEEK('2008-12-31',1) |
     * +----------------------+
     * |                   53 |
     * +----------------------+
     * <p>
     * SELECT WEEK('2019-12-30',3);
     * +----------------------+
     * | WEEK('2019-12-30',3) |
     * +----------------------+
     * |                    1 |
     * +----------------------+
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * SELECT d, WEEK(d,0), WEEK(d,1) from t1;
     * +---------------------+-----------+-----------+
     * | d                   | WEEK(d,0) | WEEK(d,1) |
     * +---------------------+-----------+-----------+
     * | 2007-01-30 21:31:07 |         4 |         5 |
     * | 1983-10-15 06:42:51 |        41 |        41 |
     * | 2011-04-21 12:34:56 |        16 |        16 |
     * | 2011-10-30 06:31:41 |        44 |        43 |
     * | 2011-01-30 14:03:25 |         5 |         4 |
     * | 2004-10-07 11:19:34 |        40 |        41 |
     * +---------------------+-----------+-----------+
     *
     * @param date
     * @return
     */
    public static Integer WEEK(Object date) {
        return Integer.parseInt((UnsolvedMysqlFunctionUtil.eval("WEEK", date).toString()));
    }

    public static Integer WEEK(Object date, int mode) {
        return Integer.parseInt((UnsolvedMysqlFunctionUtil.eval("WEEK", date, mode).toString()));
    }

    /**
     * WEEKDAY
     * Syntax
     * WEEKDAY(date)
     * Description
     * Returns the weekday index for date (0 = Monday, 1 = Tuesday, ... 6 = Sunday).
     * <p>
     * This contrasts with DAYOFWEEK() which follows the ODBC standard (1 = Sunday, 2 = Monday, ..., 7 = Saturday).
     * <p>
     * Examples
     * SELECT WEEKDAY('2008-02-03 22:23:00');
     * +--------------------------------+
     * | WEEKDAY('2008-02-03 22:23:00') |
     * +--------------------------------+
     * |                              6 |
     * +--------------------------------+
     * <p>
     * SELECT WEEKDAY('2007-11-06');
     * +-----------------------+
     * | WEEKDAY('2007-11-06') |
     * +-----------------------+
     * |                     1 |
     * +-----------------------+
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * SELECT d FROM t1 where WEEKDAY(d) = 6;
     * +---------------------+
     * | d                   |
     * +---------------------+
     * | 2011-10-30 06:31:41 |
     * | 2011-01-30 14:03:25 |
     * +---------------------+
     *
     * @param date
     * @return
     */
    public static Integer WEEKDAY(Object date) {
        return Integer.parseInt((UnsolvedMysqlFunctionUtil.eval("WEEKDAY", date).toString()));
    }

    /**
     * WEEKOFYEAR
     * Syntax
     * WEEKOFYEAR(date)
     * Description
     * Returns the calendar week of the date as a number in the range from 1 to 53. WEEKOFYEAR() is a compatibility function that is equivalent to WEEK(date,3).
     * <p>
     * Examples
     * SELECT WEEKOFYEAR('2008-02-20');
     * +--------------------------+
     * | WEEKOFYEAR('2008-02-20') |
     * +--------------------------+
     * |                        8 |
     * +--------------------------+
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * select * from t1;
     * +---------------------+
     * | d                   |
     * +---------------------+
     * | 2007-01-30 21:31:07 |
     * | 1983-10-15 06:42:51 |
     * | 2011-04-21 12:34:56 |
     * | 2011-10-30 06:31:41 |
     * | 2011-01-30 14:03:25 |
     * | 2004-10-07 11:19:34 |
     * +---------------------+
     * SELECT d, WEEKOFYEAR(d), WEEK(d,3) from t1;
     * +---------------------+---------------+-----------+
     * | d                   | WEEKOFYEAR(d) | WEEK(d,3) |
     * +---------------------+---------------+-----------+
     * | 2007-01-30 21:31:07 |             5 |         5 |
     * | 1983-10-15 06:42:51 |            41 |        41 |
     * | 2011-04-21 12:34:56 |            16 |        16 |
     * | 2011-10-30 06:31:41 |            43 |        43 |
     * | 2011-01-30 14:03:25 |             4 |         4 |
     * | 2004-10-07 11:19:34 |            41 |        41 |
     * +---------------------+---------------+-----------+
     *
     * @param date
     * @return
     */
    public static Integer WEEKOFYEAR(Object date) {
        return Integer.parseInt((UnsolvedMysqlFunctionUtil.eval("WEEKOFYEAR", date).toString()));
    }

    /**
     * YEAR
     * Syntax
     * YEAR(date)
     * Description
     * Returns the year for the given date, in the range 1000 to 9999, or 0 for the "zero" date.
     * <p>
     * Examples
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * SELECT * FROM t1;
     * +---------------------+
     * | d                   |
     * +---------------------+
     * | 2007-01-30 21:31:07 |
     * | 1983-10-15 06:42:51 |
     * | 2011-04-21 12:34:56 |
     * | 2011-10-30 06:31:41 |
     * | 2011-01-30 14:03:25 |
     * | 2004-10-07 11:19:34 |
     * +---------------------+
     * <p>
     * SELECT * FROM t1 WHERE YEAR(d) = 2011;
     * +---------------------+
     * | d                   |
     * +---------------------+
     * | 2011-04-21 12:34:56 |
     * | 2011-10-30 06:31:41 |
     * | 2011-01-30 14:03:25 |
     * +---------------------+
     * SELECT YEAR('1987-01-01');
     * +--------------------+
     * | YEAR('1987-01-01') |
     * +--------------------+
     * |               1987 |
     * +--------------------+
     *
     * @param date
     * @return
     */
    public static Integer YEAR(Date date) {
        if (date == null) {
            return null;
        }
        return date.getYear();
    }

    /**
     * YEARWEEK
     * Syntax
     * YEARWEEK(date), YEARWEEK(date,mode)
     * Description
     * Returns year and week for a date. The mode argument works exactly like the mode argument to WEEK(). The year in the result may be different from the year in the date argument for the first and the last week of the year.
     * <p>
     * Examples
     * SELECT YEARWEEK('1987-01-01');
     * +------------------------+
     * | YEARWEEK('1987-01-01') |
     * +------------------------+
     * |                 198652 |
     * +------------------------+
     * CREATE TABLE t1 (d DATETIME);
     * INSERT INTO t1 VALUES
     * ("2007-01-30 21:31:07"),
     * ("1983-10-15 06:42:51"),
     * ("2011-04-21 12:34:56"),
     * ("2011-10-30 06:31:41"),
     * ("2011-01-30 14:03:25"),
     * ("2004-10-07 11:19:34");
     * SELECT * FROM t1;
     * +---------------------+
     * | d                   |
     * +---------------------+
     * | 2007-01-30 21:31:07 |
     * | 1983-10-15 06:42:51 |
     * | 2011-04-21 12:34:56 |
     * | 2011-10-30 06:31:41 |
     * | 2011-01-30 14:03:25 |
     * | 2004-10-07 11:19:34 |
     * +---------------------+
     * 6 rows in set (0.02 sec)
     * SELECT YEARWEEK(d) FROM t1 WHERE YEAR(d) = 2011;
     * +-------------+
     * | YEARWEEK(d) |
     * +-------------+
     * |      201116 |
     * |      201144 |
     * |      201105 |
     * +-------------+
     * 3 rows in set (0.03 sec)
     *
     * @param date
     * @return
     */
    public static Integer YEARWEEK(Date date) {
        return Integer.parseInt(UnsolvedMysqlFunctionUtil.eval("YEARWEEK", date).toString());
    }

    public static Integer YEARWEEK(Date date, int mode) {
        return Integer.parseInt(UnsolvedMysqlFunctionUtil.eval("YEARWEEK", date, mode).toString());
    }
}
package io.mycat.calcite.sqlfunction;

import org.apache.calcite.MycatContext;

import java.sql.Time;

/**
 * SqlStdOperatorTable.EXTRACT
 *
 * EXTRACT
 * Syntax
 * EXTRACT(unit FROM date)
 * Contents
 * Syntax
 * Description
 * Examples
 * See Also
 * Description
 * The EXTRACT() function extracts the required unit from the date. See Date and Time Units for a complete list of permitted units.
 *
 * In MariaDB 10.0.7 and MariaDB 5.5.35, EXTRACT (HOUR FROM ...) was changed to return a value from 0 to 23, adhering to the SQL standard. Until MariaDB 10.0.6 and MariaDB 5.5.34, and in all versions of MySQL at least as of MySQL 5.7, it could return a value > 23. HOUR() is not a standard function, so continues to adhere to the old behaviour inherited from MySQL.
 *
 * Examples
 * SELECT EXTRACT(YEAR FROM '2009-07-02');
 * +---------------------------------+
 * | EXTRACT(YEAR FROM '2009-07-02') |
 * +---------------------------------+
 * |                            2009 |
 * +---------------------------------+
 *
 * SELECT EXTRACT(YEAR_MONTH FROM '2009-07-02 01:02:03');
 * +------------------------------------------------+
 * | EXTRACT(YEAR_MONTH FROM '2009-07-02 01:02:03') |
 * +------------------------------------------------+
 * |                                         200907 |
 * +------------------------------------------------+
 *
 * SELECT EXTRACT(DAY_MINUTE FROM '2009-07-02 01:02:03');
 * +------------------------------------------------+
 * | EXTRACT(DAY_MINUTE FROM '2009-07-02 01:02:03') |
 * +------------------------------------------------+
 * |                                          20102 |
 * +------------------------------------------------+
 *
 * SELECT EXTRACT(MICROSECOND FROM '2003-01-02 10:30:00.000123');
 * +--------------------------------------------------------+
 * | EXTRACT(MICROSECOND FROM '2003-01-02 10:30:00.000123') |
 * +--------------------------------------------------------+
 * |                                                    123 |
 * +--------------------------------------------------------+
 * From MariaDB 10.0.7 and MariaDB 5.5.35, EXTRACT (HOUR FROM...) returns a value from 0 to 23, as per the SQL standard. HOUR is not a standard function, so continues to adhere to the old behaviour inherited from MySQL.
 *
 * SELECT EXTRACT(HOUR FROM '26:30:00'), HOUR('26:30:00');
 * +-------------------------------+------------------+
 * | EXTRACT(HOUR FROM '26:30:00') | HOUR('26:30:00') |
 * +-------------------------------+------------------+
 * |                             2 |               26 |
 * +-------------------------------+------------------+
 */
public class ExtractFunction {
    public static long eval(Time time) {
        return MycatContext.CONTEXT.get().getLastInsertId();
    }
}
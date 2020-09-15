package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

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
 *
 * The optional precision determines the microsecond precision. See Microseconds in MariaDB.
 *
 * NOW() (or its synonyms) can be used as the default value for TIMESTAMP columns as well as, since MariaDB 10.0.1, DATETIME columns. Before MariaDB 10.0.1, it was only possible for a single TIMESTAMP column per table to contain the CURRENT_TIMESTAMP as its default.
 *
 * When displayed in the INFORMATION_SCHEMA.COLUMNS table, a default CURRENT TIMESTAMP is displayed as CURRENT_TIMESTAMP up until MariaDB 10.2.2, and as current_timestamp() from MariaDB 10.2.3, due to to MariaDB 10.2 accepting expressions in the DEFAULT clause.
 *
 * Examples
 * SELECT NOW();
 * +---------------------+
 * | NOW()               |
 * +---------------------+
 * | 2010-03-27 13:13:25 |
 * +---------------------+
 *
 * SELECT NOW() + 0;
 * +-----------------------+
 * | NOW() + 0             |
 * +-----------------------+
 * | 20100327131329.000000 |
 * +-----------------------+
 * With precision:
 *
 * SELECT CURRENT_TIMESTAMP(2);
 * +------------------------+
 * | CURRENT_TIMESTAMP(2)   |
 * +------------------------+
 * | 2018-07-10 09:47:26.24 |
 * +------------------------+
 * Used as a default TIMESTAMP:
 *
 * CREATE TABLE t (createdTS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);
 * From MariaDB 10.2.2:
 *
 * SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test'
 *   AND COLUMN_NAME LIKE '%ts%'\G
 * *************************** 1. row ***************************
 *            TABLE_CATALOG: def
 *             TABLE_SCHEMA: test
 *               TABLE_NAME: t
 *              COLUMN_NAME: ts
 *         ORDINAL_POSITION: 1
 *           COLUMN_DEFAULT: current_timestamp()
 * ...
 * <= MariaDB 10.2.1
 *
 * SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test'
 *   AND COLUMN_NAME LIKE '%ts%'\G
 * *************************** 1. row ***************************
 *            TABLE_CATALOG: def
 *             TABLE_SCHEMA: test
 *               TABLE_NAME: t
 *              COLUMN_NAME: createdTS
 *         ORDINAL_POSITION: 1
 *           COLUMN_DEFAULT: CURRENT_TIMESTAMP
 * ...
 */
public class Now1Function {
    public static String eval(int precision) {
        return ((String) UnsolvedMysqlFunctionUtil.eval("now", precision));

    }
}
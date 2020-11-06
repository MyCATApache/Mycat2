package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

/*
ADDDATE
Syntax
ADDDATE(date,INTERVAL expr unit), ADDDATE(expr,days)
Description
When invoked with the INTERVAL form of the second argument, ADDDATE() is a synonym for DATE_ADD(). The related function SUBDATE() is a synonym for DATE_SUB(). For information on the INTERVAL unit argument, see the discussion for DATE_ADD().

When invoked with the days form of the second argument, MariaDB treats it as an integer number of days to be added to expr.

Examples
SELECT DATE_ADD('2008-01-02', INTERVAL 31 DAY);
+-----------------------------------------+
| DATE_ADD('2008-01-02', INTERVAL 31 DAY) |
+-----------------------------------------+
| 2008-02-02                              |
+-----------------------------------------+

SELECT ADDDATE('2008-01-02', INTERVAL 31 DAY);
+----------------------------------------+
| ADDDATE('2008-01-02', INTERVAL 31 DAY) |
+----------------------------------------+
| 2008-02-02                             |
+----------------------------------------+
SELECT ADDDATE('2008-01-02', 31);
+---------------------------+
| ADDDATE('2008-01-02', 31) |
+---------------------------+
| 2008-02-02                |
+---------------------------+
CREATE TABLE t1 (d DATETIME);
INSERT INTO t1 VALUES
    ("2007-01-30 21:31:07"),
    ("1983-10-15 06:42:51"),
    ("2011-04-21 12:34:56"),
    ("2011-10-30 06:31:41"),
    ("2011-01-30 14:03:25"),
    ("2004-10-07 11:19:34");
SELECT d, ADDDATE(d, 10) from t1;
+---------------------+---------------------+
| d                   | ADDDATE(d, 10)      |
+---------------------+---------------------+
| 2007-01-30 21:31:07 | 2007-02-09 21:31:07 |
| 1983-10-15 06:42:51 | 1983-10-25 06:42:51 |
| 2011-04-21 12:34:56 | 2011-05-01 12:34:56 |
| 2011-10-30 06:31:41 | 2011-11-09 06:31:41 |
| 2011-01-30 14:03:25 | 2011-02-09 14:03:25 |
| 2004-10-07 11:19:34 | 2004-10-17 11:19:34 |
+---------------------+---------------------+

SELECT d, ADDDATE(d, INTERVAL 10 HOUR) from t1;
+---------------------+------------------------------+
| d                   | ADDDATE(d, INTERVAL 10 HOUR) |
+---------------------+------------------------------+
| 2007-01-30 21:31:07 | 2007-01-31 07:31:07          |
| 1983-10-15 06:42:51 | 1983-10-15 16:42:51          |
| 2011-04-21 12:34:56 | 2011-04-21 22:34:56          |
| 2011-10-30 06:31:41 | 2011-10-30 16:31:41          |
| 2011-01-30 14:03:25 | 2011-01-31 00:03:25          |
| 2004-10-07 11:19:34 | 2004-10-07 21:19:34          |
+---------------------+------------------------------+

 */
public class AddDateFunction {


    public static String eval(String arg0,int days) {
        if (arg0 != null) {
            return  UnsolvedMysqlFunctionUtil.eval("DATE_ADD", arg0,days).toString();
        }
        return null;
    }
}
package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

/**
 * Syntax
 * ADDTIME(expr1,expr2)
 * Description
 * ADDTIME() adds expr2 to expr1 and returns the result. expr1 is a time or datetime expression, and expr2 is a time expression.
 * <p>
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
 */
public class AddTimeFunction {
    public static String eval(String arg0, String arg1) {
        if (arg0 != null && arg1 != null) {
            return UnsolvedMysqlFunctionUtil.eval("ADDTIME", arg0, arg1).toString();
        }
        return null;
    }
}
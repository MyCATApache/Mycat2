package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

public  class BitLengthFunction {
        public static Integer eval(String arg0) {
            if (arg0 != null) {
                return ((Number) UnsolvedMysqlFunctionUtil.eval("BIT_LENGTH", arg0)).intValue();
            }
            return null;
        }
    }

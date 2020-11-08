package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.util.Objects;

public  class CONVFunction {
        public static String eval(String arg0, String arg1, String arg2) {
            return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("CONV", arg0, arg1, arg2)));
        }
    }

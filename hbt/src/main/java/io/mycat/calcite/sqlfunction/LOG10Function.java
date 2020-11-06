package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.util.Objects;

public  class LOG10Function {
        public static String eval(String arg0) {
            return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("LOG10", arg0)));
        }
    }
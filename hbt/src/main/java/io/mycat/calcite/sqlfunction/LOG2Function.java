package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.util.Objects;

public class LOG2Function {
    public static String eval(String arg0) {
        return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("LOG2", arg0)));
    }
}

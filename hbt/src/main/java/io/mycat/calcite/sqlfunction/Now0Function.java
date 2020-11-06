package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.util.Objects;

public class Now0Function {
    public static String eval() {
        return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("now")));
    }
}
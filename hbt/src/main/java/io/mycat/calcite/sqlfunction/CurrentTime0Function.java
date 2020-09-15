package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.util.Objects;

/**
 * CURRENT_TIME
 * Syntax
 * CURRENT_TIME
 * CURRENT_TIME([precision])
 * Description
 * CURRENT_TIME and CURRENT_TIME() are synonyms for CURTIME().
 *
 * io.mycat.calcite.sqlfunction.CurrentTime1Function
 */
public class CurrentTime0Function {
    public static String eval() {
        return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("CURTIME")));
    }
}
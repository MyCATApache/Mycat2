package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.time.LocalTime;
import java.util.Objects;

/**
 * CURRENT_TIME
 * Syntax
 * CURRENT_TIME
 * CURRENT_TIME([precision])
 * Description
 * CURRENT_TIME and CURRENT_TIME() are synonyms for CURTIME().
 */
public class CurrentTime1Function {
    public static String eval(int precision) {
        return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("CURTIME",precision)));
    }
}
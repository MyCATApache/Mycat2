package io.mycat.calcite.sqlfunction;

/**
 * CURRENT_TIMESTAMP
 * Syntax
 * CURRENT_TIMESTAMP
 * CURRENT_TIMESTAMP([precision])
 * Description
 * CURRENT_TIMESTAMP and CURRENT_TIMESTAMP() are synonyms for NOW().
 *
 * See Also
 * Microseconds in MariaDB
 * The TIMESTAMP data type
 */
public class CurrentTimestamp1Function {
    public static String eval(int arg) {
        return Now1Function.eval(arg);
    }

}
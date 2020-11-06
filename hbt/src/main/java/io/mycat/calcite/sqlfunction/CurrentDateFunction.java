package io.mycat.calcite.sqlfunction;

import java.time.LocalDate;

/**
 * CURRENT_DATE
 * Syntax
 * CURRENT_DATE, CURRENT_DATE()
 * Description
 * CURRENT_DATE and CURRENT_DATE() are synonyms for CURDATE().
 */
public class CurrentDateFunction {
    public static String eval() {
        return LocalDate.now().toString();
    }
}
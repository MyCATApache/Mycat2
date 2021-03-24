/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.sqlfunction.datefunction;

import com.github.sisyphsu.dateparser.DateParserUtils;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SecToDateFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(SecToDateFunction.class,
            "strToDate");
    public static SecToDateFunction INSTANCE = new SecToDateFunction();

    public SecToDateFunction() {
        super("STR_TO_DATE",
                scalarFunction
        );
    }

    public static String strToDate(String str, String format) {
        if (str == null || format == null) {
            return null;
        }
        try {
            String javaPattern = format;
            for (Map.Entry<String, String> entry : mysqlUnitMap.entrySet()) {
                javaPattern = javaPattern.replaceAll(entry.getKey(), entry.getValue());
            }
            Date date = DateParserUtils.parseDate(str);
            if (format.toLowerCase().contains("%y")) {
                return DateFormatUtils.format(date, "yyyy-MM-dd");
            } else {
                return DateFormatUtils.format(date, "HH:mm:ss");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * copy from linux_china
     * h2-functions-4-mysql
     */
    public static final Map<String, String> mysqlUnitMap = mysqlToJavaDateFormat();

    private static Map<String, String> mysqlToJavaDateFormat() {
        Map<String, String> convert = new HashMap<>();
        convert.put("%a", "E");
        convert.put("%b", "M");
        convert.put("%c", "M");
        convert.put("%d", "dd");
        convert.put("%e", "d");
        convert.put("%f", "S");
        convert.put("%H", "HH");
        convert.put("%h", "H");
        convert.put("%I", "h");
        convert.put("%i", "mm");
        convert.put("%J", "D");
        convert.put("%k", "h");
        convert.put("%l", "h");
        convert.put("%M", "M");
        convert.put("%m", "MM");
        convert.put("%p", "a");
        convert.put("%r", "hh:mm:ss a");
        convert.put("%s", "ss");
        convert.put("%S", "ss");
        convert.put("%T", "HH:mm:ss");
        convert.put("%U", "w");
        convert.put("%u", "w");
        convert.put("%V", "w");
        convert.put("%v", "w");
        convert.put("%W", "EEE");
        convert.put("%w", "F");
        convert.put("%Y", "yyyy");
        convert.put("%y", "yy");
        return convert;
    }
}

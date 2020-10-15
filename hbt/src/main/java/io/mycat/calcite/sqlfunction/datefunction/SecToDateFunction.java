package io.mycat.calcite.sqlfunction.datefunction;

import com.github.sisyphsu.dateparser.DateParserUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.time.*;
import java.time.format.*;
import java.time.temporal.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
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

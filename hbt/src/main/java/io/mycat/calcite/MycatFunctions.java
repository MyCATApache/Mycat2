package io.mycat.calcite;

import org.apache.calcite.linq4j.function.Parameter;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class MycatFunctions {
    public static class DateFormatFunction {
        public static String eval(@Parameter(name = "date") String date, @Parameter(name = "format") String format) {
            return LocalDate.parse(date).format(DateTimeFormatter.ofPattern(format));
        }
    }
}
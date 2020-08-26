package io.mycat.calcite;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.replica.ReplicaSelectorRuntime;
import org.apache.calcite.linq4j.function.Parameter;

import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;

public class MycatFunctions {
    public static class DateFormatFunction {
        public static String eval(@Parameter(name = "date") String dateText, @Parameter(name = "format") String format) {
            LocalDate date = LocalDate.parse(dateText, BASIC_ISO_DATE);

            Locale locale = Locale.getDefault();
            format = format.replaceAll("%a", date.getDayOfWeek().getDisplayName(TextStyle.SHORT, locale));
            format = format.replaceAll("%b", date.getMonth().getDisplayName(TextStyle.SHORT, locale));
            format = format.replaceAll("%c", date.getMonthValue() + "");
            format = format.replaceAll("%Y", String.format("%04d", date.getYear()));
            format = format.replaceAll("%y", String.format("%02d", date.getYear()));
            format = format.replaceAll("%m", String.format("%02d", date.getMonthValue()));
            format = format.replaceAll("%M", date.getMonth().getDisplayName(TextStyle.FULL, locale));
            format = format.replaceAll("%d", String.format("%02d", date.getDayOfMonth()));
            format = format.replaceAll("%e", String.format("%01d", date.getDayOfMonth()));
            format = format.replaceAll("%c", String.format("%01d", date.getMonthValue()));

            if (!format.contains("%")) {
                return format;
            }
            String datasource = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByRandom();
            try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(datasource)) {
                RowBaseIterator rowBaseIterator = connection.executeQuery("select data_format('" + dateText + "','" + format + "')");
                rowBaseIterator.next();
                return rowBaseIterator.getString(1);
            }
        }
    }
    public static class UnixTimestampFunction {
        public static Long eval(@Parameter(name = "date") String dateText) {
            String datasource = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByRandom();
            try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(datasource)) {
                RowBaseIterator rowBaseIterator = connection.executeQuery("select UNIX_TIMESTAMP('" + dateText + "')");
                rowBaseIterator.next();
                return rowBaseIterator.getLong(1);
            }
        }
    }
    public static class ConcatFunction {
        public static String eval(String arg0,String arg1) {
           return arg0+arg1;
        }
    }
}
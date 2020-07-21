package io.mycat.calcite;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.replica.ReplicaSelectorRuntime;
import org.apache.calcite.linq4j.function.Parameter;

import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Locale;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;

public class MycatFunctions {
    public static class DateFormatFunction {
        public static String eval(@Parameter(name = "date") String dateText, @Parameter(name = "format") String format) {
            LocalDate date = LocalDate.parse(dateText,BASIC_ISO_DATE);

            Locale locale = Locale.getDefault();
            format= format.replace("%a",date.getDayOfWeek().getDisplayName(TextStyle.SHORT,locale));
            format =format.replace("%b",date.getMonth().getDisplayName(TextStyle.SHORT,locale));
            format= format.replace("%c",date.getMonthValue()+"");
            format= format.replace("%Y",String.format("%04d",date.getYear()));
            format= format.replace("%y",String.format("%02d",date.getYear()));
            format= format.replace("%m",String.format("%02d", date.getMonthValue()));
            format= format.replace("%M",date.getMonth().getDisplayName(TextStyle.FULL,locale));
            format= format.replace("%d",String.format("%02d", date.getDayOfMonth()));
            format= format.replace("%e",String.format("%01d", date.getDayOfMonth()));
            format= format.replace("%c",String.format("%01d", date.getMonthValue()));

            if (!format.contains("%")){
                return format;
            }
            String datasource = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByRandom();
            try(DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(datasource)){
                RowBaseIterator rowBaseIterator = connection.executeQuery("select data_format('" + dateText +"','"+format+ "')");
               return rowBaseIterator.getString(1);
            }
        }
    }

}
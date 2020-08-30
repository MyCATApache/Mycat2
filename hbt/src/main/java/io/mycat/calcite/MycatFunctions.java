package io.mycat.calcite;

import org.apache.calcite.linq4j.function.Parameter;

import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Objects;

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
            return Objects.toString(UnsolvedMysqlFunctionUtil.eval("data_format", dateText, format));
        }
    }

    public static class UnixTimestampFunction {
        public static Long eval(@Parameter(name = "date") String dateText) {
            return ((Number) UnsolvedMysqlFunctionUtil.eval("UNIX_TIMESTAMP", dateText)).longValue();
        }
    }
    public static class ConcatFunction {
        public static String eval(String[] args) {
            for (String arg : args) {
                if (arg == null){
                    return null;
                }
            }
            return String.join("",args);
        }
    }
    public static class Concat2Function {
        public static String eval(String arg0, String arg1) {
            if (arg0 == null) {
                return null;
            }
            if (arg1 == null) {
                return null;
            }
            return arg0 + arg1;
        }
    }

    public static class Concat3Function {
        public static String eval(String arg0, String arg1, String arg2) {
            if (arg0 == null) {
                return null;
            }
            if (arg1 == null) {
                return null;
            }
            if (arg2 == null) {
                return null;
            }
            return arg0 + arg1 + arg2;
        }
    }

    public static class Concat4Function {
        public static String eval(String arg0, String arg1, String arg2, String arg3) {
            if (arg0 == null) {
                return arg0;
            }
            if (arg1 == null) {
                return null;
            }
            if (arg2 == null) {
                return null;
            }
            if (arg3 == null) {
                return null;
            }
            return arg0 + arg1 + arg2 + arg3;
        }
    }
    public static class ConcatWSFunction {
        public static String eval(String spilt,String[] args) {
            for (String arg : args) {
                if (arg == null){
                    return null;
                }
            }
            return String.join(spilt,args);
        }
    }
    public static class PiFunction {
        public static double eval() {
            return Math.PI;
        }
    }

    public static class CONVFunction {
        public static String eval(String arg0, String arg1, String arg2) {
            return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("CONV", arg0, arg1, arg2)));
        }
    }

    public static class CRC32Function {
        public static String eval(String arg0) {
            return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("crc32", arg0)));
        }
    }

    public static class LOGFunction {
        public static String eval(String arg0) {
            return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("LOG", arg0)));
        }
    }

    public static class LOG10Function {
        public static String eval(String arg0) {
            return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("LOG10", arg0)));
        }
    }

    public static class LOG2Function {
        public static String eval(String arg0) {
            return ((String) Objects.toString(UnsolvedMysqlFunctionUtil.eval("LOG2", arg0)));
        }
    }
    public static class BitWiseOrFunction {
        public static Long eval(Long arg0,Long arg1) {
            if (arg0 == null){
                return null;
            }
            if (arg1 == null){
                return null;
            }
            return arg0|arg1;
        }
    }
}
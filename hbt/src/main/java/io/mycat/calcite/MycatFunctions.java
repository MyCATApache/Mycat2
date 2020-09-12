package io.mycat.calcite;

import io.mycat.hbt4.ExecutorImplementor;
import io.mycat.hbt4.MycatContext;
import org.apache.calcite.linq4j.function.Parameter;

import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;

public class MycatFunctions {




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
        public static String eval(String spilt, String[] args) {
            for (String arg : args) {
                if (arg == null) {
                    return null;
                }
            }
            return String.join(spilt, args);
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
        public static Long eval(Long arg0, Long arg1) {
            if (arg0 == null) {
                return null;
            }
            if (arg1 == null) {
                return null;
            }
            return arg0 | arg1;
        }
    }

    public static class BinFunction {
        public static String eval(Long arg0) {
            if (arg0 != null) {
                return Long.toBinaryString(arg0);
            }
            return null;
        }
    }

    public static class BitLengthFunction {
        public static Integer eval(String arg0) {
            if (arg0 != null) {
                return ((Number) UnsolvedMysqlFunctionUtil.eval("BIT_LENGTH", arg0)).intValue();
            }
            return null;
        }
    }

    public static class CharFunction {
        public static String eval(String... args) {
            ArrayList<Object> list = new ArrayList<>();
            for (Object arg : args) {
                if (arg != null) {
                    list.add(arg);
                }
            }
            return ((String) UnsolvedMysqlFunctionUtil.eval("char", list.toArray(new String[list.size()])));
        }
    }

    public static class LAST_INSERT_IDFunction {
        public static Long eval() {
            return MycatContext.CONTEXT.get().getLastInsertId();
        }
    }

    public static class Char2Function {
        public static String eval(String arg0, String arg1) {
            return CharFunction.eval(arg0, arg1);
        }
    }

    public static class Char3Function {
        public static String eval(String... args) {
            ArrayList<Object> list = new ArrayList<>();
            for (Object arg : args) {
                if (arg != null) {
                    list.add(arg);
                }
            }
            return ((String) UnsolvedMysqlFunctionUtil.eval("char", list.toArray(new String[list.size()])));
        }
    }
}
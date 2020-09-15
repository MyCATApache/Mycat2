package io.mycat.calcite.sqlfunction;

public class ConcatWSFunction {
    public static String eval(String spilt, String[] args) {
        for (String arg : args) {
            if (arg == null) {
                return null;
            }
        }
        return String.join(spilt, args);
    }
}

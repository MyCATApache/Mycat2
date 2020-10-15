package io.mycat.calcite.sqlfunction;

public  class BinFunction {
        public static String eval(Long arg0) {
            if (arg0 != null) {
                return Long.toBinaryString(arg0);
            }
            return null;
        }
    }

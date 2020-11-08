package io.mycat.calcite.sqlfunction;

public  class BitWiseOrFunction {
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

  
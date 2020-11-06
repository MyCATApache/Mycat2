package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;

import java.util.ArrayList;

public  class CharFunction {
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
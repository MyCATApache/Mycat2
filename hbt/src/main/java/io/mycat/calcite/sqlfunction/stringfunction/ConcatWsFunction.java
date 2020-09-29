package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;

import java.util.ArrayList;

public class ConcatWsFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ConcatWsFunction.class,
            "concatWs");
    public static final ConcatWsFunction INSTANCE = new ConcatWsFunction();

    public ConcatWsFunction() {
        super("concat_ws", scalarFunction);
    }

    public static String concatWs(String... n) {
        if (n == null || n.length == 0) {
            return null;
        }
        String seq = n[0];
        if (seq == null) return null;
        ArrayList<String> list = new ArrayList<>(n.length);
        for (int i = 1; i < n.length; i++) {
            String s = n[i];
            if (s != null){
                list.add(s);
            }
        }
        return String.join(seq,list);
    }
}
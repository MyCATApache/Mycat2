package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class RepeatFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(RepeatFunction.class,
            "repeat");

    public static final RepeatFunction INSTANCE = new RepeatFunction();

    public RepeatFunction() {
        super("REPEAT", scalarFunction);
    }

    public static String repeat(String str, Integer count) {
        if (str == null || count == null) {
            return null;
        }
        if (count<1){
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class LeftFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(LeftFunction.class,
            "left");

    public static final LeftFunction INSTANCE = new LeftFunction();

    public LeftFunction() {
        super("left", scalarFunction);
    }

    public static String left(String str,Integer len) {
        if (str == null || len == null) {
            return null;
        }
        int size = str.length();
        if (len >= size) {
            return str;
        }
        return str.substring(0, len);
    }
}
package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class LowerFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(LowerFunction.class,
            "lower");

    public static final LowerFunction INSTANCE = new LowerFunction();

    public LowerFunction() {
        super("lower", scalarFunction);
    }

    public static String lower(String str) {
        if (str == null) {
            return null;
        }
        return str.toLowerCase();
    }
}
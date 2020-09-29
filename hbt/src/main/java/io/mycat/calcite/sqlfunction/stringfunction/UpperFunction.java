package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class UpperFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UpperFunction.class,
            "upper");

    public static final UpperFunction INSTANCE = new UpperFunction();

    public UpperFunction() {
        super("upper", scalarFunction);
    }

    public static String upper(String str) {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return "";
        }
     return str.toUpperCase();
    }
}
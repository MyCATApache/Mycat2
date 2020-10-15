package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class ReverseFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ReverseFunction.class,
            "reverse");

    public static final ReverseFunction INSTANCE = new ReverseFunction();

    public ReverseFunction() {
        super("Reverse", scalarFunction);
    }

    public static String reverse(String str) {
        if (str == null) {
            return null;
        }
       return new StringBuilder(str).reverse().toString();
    }
}
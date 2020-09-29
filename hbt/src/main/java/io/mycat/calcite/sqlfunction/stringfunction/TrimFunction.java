package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.avatica.util.Base64;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class TrimFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TrimFunction.class,
            "trim");

    public static final TrimFunction INSTANCE = new TrimFunction();

    public TrimFunction() {
        super("trim", scalarFunction);
    }

    public static String trim(String str) {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return "";
        }
     return str.trim();
    }
}
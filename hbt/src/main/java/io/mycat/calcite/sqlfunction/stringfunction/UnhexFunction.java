package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class UnhexFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UnhexFunction.class,
            "unhex");

    public static final UnhexFunction INSTANCE = new UnhexFunction();

    public UnhexFunction() {
        super("unhex", scalarFunction);
    }

    public static ByteString unhex(String str) {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return ByteString.EMPTY;
        }
     return new ByteString (ByteString.parse(str,16));
    }
}
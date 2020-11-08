package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.avatica.util.Base64;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class ToBase64Function extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ToBase64Function.class,
            "toBase64");

    public static final ToBase64Function INSTANCE = new ToBase64Function();

    public ToBase64Function() {
        super("TO_BASE64", scalarFunction);
    }

    public static String toBase64(String str) {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return "";
        }
     return Base64.encodeBytes(str.getBytes());
    }
}
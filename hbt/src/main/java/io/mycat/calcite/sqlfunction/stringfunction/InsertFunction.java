package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class InsertFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(InsertFunction.class,
            "insert");

    public static final InsertFunction INSTANCE = new InsertFunction();

    public InsertFunction() {
        super("insert", scalarFunction);
    }

    public static String insert(String str, Integer pos, Integer len, String newstr) {
        if (str == null || pos == null || len == null || newstr == null) {
            return null;
        }
        int orginalLen = str.length();
        if (pos <= 0 || pos > orginalLen) {
            return str;
        }
        if (len < 0 || pos + len > orginalLen) {
            return str.substring(0, pos - 1) + newstr + str.substring(orginalLen );
        } else {
            return str.substring(0, pos - 1) + newstr.substring(0, len) + str.substring(pos - 1 + len);
        }
    }
}
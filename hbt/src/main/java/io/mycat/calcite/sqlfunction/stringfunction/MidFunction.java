package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class MidFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(MidFunction.class,
            "mid");

    public static final MidFunction INSTANCE = new MidFunction();

    public MidFunction() {
        super("mid", scalarFunction);
    }

    public static String mid(String str, Integer pos, Integer len) {
        if (str == null || pos == null || len == null) {
            return null;
        }
        return str.substring(pos,len);
    }
}
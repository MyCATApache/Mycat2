package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class ReplaceFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ReplaceFunction.class,
            "repeat");

    public static final ReplaceFunction INSTANCE = new ReplaceFunction();

    public ReplaceFunction() {
        super(new SqlIdentifier("REPLACE", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR),  InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static String replace(String str, String from_str,String to_str) {
        if (str == null || from_str == null||to_str == null) {
            return null;
        }
        return str.replace(from_str, to_str);
    }
}
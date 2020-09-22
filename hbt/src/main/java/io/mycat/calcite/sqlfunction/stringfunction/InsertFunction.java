package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class InsertFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(InsertFunction.class,
            "insert");

    public static final InsertFunction INSTANCE = new InsertFunction();

    public InsertFunction() {
        super(new SqlIdentifier("insert", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR),  InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
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
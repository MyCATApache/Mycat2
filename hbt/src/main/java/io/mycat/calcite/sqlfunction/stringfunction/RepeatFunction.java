package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class RepeatFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(RepeatFunction.class,
            "repeat");

    public static final RepeatFunction INSTANCE = new RepeatFunction();

    public RepeatFunction() {
        super(new SqlIdentifier("REPEAT", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR),  InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static String repeat(String str, Integer count) {
        if (str == null || count == null) {
            return null;
        }
        if (count<1){
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
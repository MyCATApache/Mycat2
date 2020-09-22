package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class LowerFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(InstrFunction.class,
            "lower");

    public static final InstrFunction INSTANCE = new InstrFunction();

    public LowerFunction() {
        super(new SqlIdentifier("lower", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR),  InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static String lower(String str) {
        if (str == null) {
            return null;
        }
        return str.toLowerCase();
    }
}
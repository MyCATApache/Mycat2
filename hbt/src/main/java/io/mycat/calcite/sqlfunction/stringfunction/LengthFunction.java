package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class LengthFunction  extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(InstrFunction.class,
            "length");

    public static final InstrFunction INSTANCE = new InstrFunction();

    public LengthFunction() {
        super(new SqlIdentifier("length", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.INTEGER),  InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static Integer length(String str) {
        if (str == null) {
            return null;
        }
        return str.length();
    }
}
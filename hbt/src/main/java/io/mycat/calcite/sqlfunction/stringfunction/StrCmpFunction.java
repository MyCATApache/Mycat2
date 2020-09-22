package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class StrCmpFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(StrCmpFunction.class,
            "strcmp");

    public static final StrCmpFunction INSTANCE = new StrCmpFunction();

    public StrCmpFunction() {
        super(new SqlIdentifier("strcmp", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.INTEGER), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING,SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static Integer strcmp(String str,String str2) {
        if (str == null||str2==null) {
            return null;
        }
        return str.compareTo(str2);
    }
}
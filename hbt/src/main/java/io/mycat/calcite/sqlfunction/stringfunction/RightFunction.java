package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class RightFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(RightFunction.class,
            "right");

    public static final RightFunction INSTANCE = new RightFunction();

    public RightFunction() {
        super(new SqlIdentifier("right", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING,SqlTypeFamily.INTEGER),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static String right(String str,Integer len) {
        if (str == null||len==null) {
            return null;
        }
        int size = str.length();
        if (len >= size) {
            return str;
        }
        return str.substring(size - len, size);
    }
}
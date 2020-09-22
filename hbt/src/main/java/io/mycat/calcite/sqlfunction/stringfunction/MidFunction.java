package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class MidFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(MidFunction.class,
            "mid");

    public static final MidFunction INSTANCE = new MidFunction();

    public MidFunction() {
        super(new SqlIdentifier("mid", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR),  InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER,  SqlTypeFamily.INTEGER),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static String mid(String str, Integer pos, Integer len) {
        if (str == null || pos == null || len == null) {
            return null;
        }
        return str.substring(pos,len);
    }
}
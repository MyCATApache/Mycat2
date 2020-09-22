package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class UncompressedLengthFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UncompressedLengthFunction.class,
            "uncompressedLength");

    public static final UncompressedLengthFunction INSTANCE = new UncompressedLengthFunction();

    public UncompressedLengthFunction() {
        super(new SqlIdentifier("UNCOMPRESSED_LENGTH", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.INTEGER), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static Integer uncompressedLength(String str) {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return 0;
        }
        throw new UnsupportedOperationException("uncompressedLength");
    }
}
package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class PositionFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(PositionFunction.class,
            "position");

    public static final PositionFunction INSTANCE = new PositionFunction();

    public PositionFunction() {
        super(new SqlIdentifier("POSITION", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.INTEGER), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static Integer position(String substr,String str) {
      return LocateFunction.locate(substr,str);
    }
}
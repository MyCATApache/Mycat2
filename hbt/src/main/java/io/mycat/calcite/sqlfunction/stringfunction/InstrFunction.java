package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class InstrFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(InstrFunction.class,
            "instr");

    public static final InstrFunction INSTANCE = new InstrFunction();

    public InstrFunction() {
        super(new SqlIdentifier("instr", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.INTEGER),  InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING,  SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static Integer instr(String str,String substr) {
        if (str == null || substr == null) {
            return null;
        }
        return str.indexOf(substr)+1;
    }
}
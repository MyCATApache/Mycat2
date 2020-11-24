package io.mycat.calcite.sqlfunction.mathfunction;

import io.mycat.calcite.MycatScalarFunction;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class Log2Function extends MycatSqlDefinedFunction {
    public static Log2Function INSTANCE = new Log2Function();

    public Log2Function() {
        super("LOG2", ReturnTypes.DOUBLE, InferTypes.FIRST_KNOWN, OperandTypes.NUMERIC,
                MycatScalarFunction.create(Log2Function.class, "log2", 1), SqlFunctionCategory.SYSTEM);
    }

    public static Double log2(Double number) {
        return Math.log(number)/Math.log(2);
    }
}

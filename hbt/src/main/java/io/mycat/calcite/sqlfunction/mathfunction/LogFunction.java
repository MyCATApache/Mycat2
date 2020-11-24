package io.mycat.calcite.sqlfunction.mathfunction;

import io.mycat.calcite.MycatScalarFunction;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class LogFunction extends MycatSqlDefinedFunction {
    public static LogFunction INSTANCE = new LogFunction();

    public LogFunction() {
        super("Log", ReturnTypes.DOUBLE, InferTypes.FIRST_KNOWN, OperandTypes.ANY,
                MycatScalarFunction.create(LogFunction.class, "log", 1), SqlFunctionCategory.SYSTEM);
    }

    public static Double log(Number number) {
        return Math.log(number.doubleValue());
    }
}

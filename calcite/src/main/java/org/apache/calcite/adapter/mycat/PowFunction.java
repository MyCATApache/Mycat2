package org.apache.calcite.adapter.mycat;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class PowFunction extends MycatSqlDefinedFunction {
    public static PowFunction INSTANCE = new PowFunction();

    public PowFunction() {
        super("POW", ReturnTypes.ARG0, InferTypes.FIRST_KNOWN, OperandTypes.SAME_VARIADIC, null, SqlFunctionCategory.NUMERIC);
    }
}

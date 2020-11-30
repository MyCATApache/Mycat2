package org.apache.calcite.adapter.mycat;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class ATanFunction extends MycatSqlDefinedFunction {
    public static ATanFunction INSTANCE = new ATanFunction();

    public ATanFunction() {
        super("ATAN2", ReturnTypes.ARG0, InferTypes.FIRST_KNOWN, OperandTypes.VARIADIC, null, SqlFunctionCategory.NUMERIC);
    }
}

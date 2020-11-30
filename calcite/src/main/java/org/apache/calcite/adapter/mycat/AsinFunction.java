package org.apache.calcite.adapter.mycat;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class AsinFunction extends MycatSqlDefinedFunction {
    public static AsinFunction INSTANCE = new AsinFunction();

    public AsinFunction() {
        super("ASIN", ReturnTypes.ARG0, InferTypes.FIRST_KNOWN, OperandTypes.VARIADIC, null, SqlFunctionCategory.NUMERIC);
    }
}

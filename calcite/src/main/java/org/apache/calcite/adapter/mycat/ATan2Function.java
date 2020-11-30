package org.apache.calcite.adapter.mycat;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class ATan2Function extends MycatSqlDefinedFunction {
    public static ATan2Function INSTANCE = new ATan2Function();

    public ATan2Function() {
        super("ATAN2", ReturnTypes.ARG0, InferTypes.FIRST_KNOWN, OperandTypes.VARIADIC, null, SqlFunctionCategory.NUMERIC);
    }
}

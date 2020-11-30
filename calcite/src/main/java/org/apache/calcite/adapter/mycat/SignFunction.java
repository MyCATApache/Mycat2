package org.apache.calcite.adapter.mycat;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class SignFunction extends MycatSqlDefinedFunction {
    public static SignFunction INSTANCE = new SignFunction();

    public SignFunction() {
        super("SIGN", ReturnTypes.ARG0, InferTypes.FIRST_KNOWN, OperandTypes.NUMERIC, null, SqlFunctionCategory.NUMERIC);
    }
}

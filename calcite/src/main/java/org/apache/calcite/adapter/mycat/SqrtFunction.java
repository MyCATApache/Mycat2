package org.apache.calcite.adapter.mycat;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class SqrtFunction extends MycatSqlDefinedFunction {
    public static SqrtFunction INSTANCE = new SqrtFunction();

    public SqrtFunction() {
        super("SQRT", ReturnTypes.ARG0, InferTypes.FIRST_KNOWN, OperandTypes.NUMERIC, null, SqlFunctionCategory.NUMERIC);
    }
}

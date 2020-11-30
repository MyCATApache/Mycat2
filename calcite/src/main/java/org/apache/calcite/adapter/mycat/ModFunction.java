package org.apache.calcite.adapter.mycat;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class ModFunction extends MycatSqlDefinedFunction {
    public static ModFunction INSTANCE = new ModFunction();

    public ModFunction() {
        super("MOD", ReturnTypes.ARG0, InferTypes.FIRST_KNOWN, OperandTypes.SAME_VARIADIC, null, SqlFunctionCategory.NUMERIC);
    }
}

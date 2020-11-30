package org.apache.calcite.adapter.mycat;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class PowerFunction extends MycatSqlDefinedFunction {
    public static PowerFunction INSTANCE = new PowerFunction();

    public PowerFunction() {
        super("POWER", ReturnTypes.ARG0, InferTypes.FIRST_KNOWN, OperandTypes.NUMERIC, null, SqlFunctionCategory.NUMERIC);
    }
}

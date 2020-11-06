package org.apache.calcite.mycat;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class MycatGlobalValueFunction extends MycatSqlDefinedFunction {

    public static MycatGlobalValueFunction INSTANCE = new MycatGlobalValueFunction();

    public MycatGlobalValueFunction() {
        super("MYCATGLOBALVALUE", ReturnTypes.ARG0,InferTypes.ANY_NULLABLE, OperandTypes.ANY, null,
                SqlFunctionCategory.SYSTEM);
    }

}


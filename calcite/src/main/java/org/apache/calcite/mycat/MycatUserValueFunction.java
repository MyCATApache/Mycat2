package org.apache.calcite.mycat;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class MycatUserValueFunction extends MycatSqlDefinedFunction {

    public static MycatUserValueFunction INSTANCE = new MycatUserValueFunction();

    public MycatUserValueFunction() {
        super("MYCATUSERVALUE", ReturnTypes.ARG0,InferTypes.ANY_NULLABLE, OperandTypes.ANY, null,
                SqlFunctionCategory.SYSTEM);
    }

}


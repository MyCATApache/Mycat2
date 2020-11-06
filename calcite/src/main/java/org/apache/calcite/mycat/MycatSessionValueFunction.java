package org.apache.calcite.mycat;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class MycatSessionValueFunction extends MycatSqlDefinedFunction {

    public static MycatSessionValueFunction INSTANCE = new MycatSessionValueFunction();

    public MycatSessionValueFunction() {
        super("MYCATSESSIONVALUE", ReturnTypes.ARG0,InferTypes.ANY_NULLABLE, OperandTypes.ANY, null,
                SqlFunctionCategory.SYSTEM);
    }

}


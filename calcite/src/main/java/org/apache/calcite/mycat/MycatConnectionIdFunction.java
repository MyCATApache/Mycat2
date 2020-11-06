package org.apache.calcite.mycat;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class MycatConnectionIdFunction extends MycatSqlDefinedFunction {
    public final static MycatConnectionIdFunction INSTANCE = new MycatConnectionIdFunction();

    public MycatConnectionIdFunction() {
        super("CONNECTION_ID",
                ReturnTypes.BIGINT_NULLABLE,
                InferTypes.RETURN_TYPE, OperandTypes.NILADIC, null, SqlFunctionCategory.SYSTEM);
    }
}

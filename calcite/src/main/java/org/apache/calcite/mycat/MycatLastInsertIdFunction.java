package org.apache.calcite.mycat;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public class MycatLastInsertIdFunction extends MycatSqlDefinedFunction {
    public final static MycatLastInsertIdFunction INSTANCE = new MycatLastInsertIdFunction();

    public MycatLastInsertIdFunction() {
        super("LAST_INSERT_ID",
                ReturnTypes.BIGINT_NULLABLE,
                InferTypes.RETURN_TYPE, OperandTypes.NILADIC, null, SqlFunctionCategory.SYSTEM);
    }
}

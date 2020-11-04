package org.apache.calcite.mycat;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public class MycatDatabaseFunction extends MycatSqlDefinedFunction {
    public final static MycatDatabaseFunction INSTANCE = new MycatDatabaseFunction();

    public MycatDatabaseFunction() {
        super("DATABASE",
                ReturnTypes.explicit(SqlTypeName.VARCHAR),
                InferTypes.RETURN_TYPE, OperandTypes.NILADIC, null, SqlFunctionCategory.SYSTEM);
    }
}

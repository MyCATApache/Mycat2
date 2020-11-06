package org.apache.calcite.mycat;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class MycatUserFunction extends MycatSqlDefinedFunction {
    public final static MycatUserFunction INSTANCE = new MycatUserFunction();

    public MycatUserFunction() {
        super("USER",
                ReturnTypes.VARCHAR_2000,
                InferTypes.RETURN_TYPE, OperandTypes.NILADIC, null, SqlFunctionCategory.SYSTEM);
    }
}

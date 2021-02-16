package io.mycat.calcite.sqlfunction.infofunction;

import io.mycat.beans.mysql.MySQLVersion;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public class MycatVersionFunction extends MycatSqlDefinedFunction {
    public final static MycatVersionFunction INSTANCE = new MycatVersionFunction();

    public MycatVersionFunction() {
        super("VERSION",
                ReturnTypes.explicit(SqlTypeName.VARCHAR),
                InferTypes.RETURN_TYPE, OperandTypes.NILADIC,
                ScalarFunctionImpl.create(MycatVersionFunction.class,"version"),
                SqlFunctionCategory.SYSTEM);
    }

    public static String version(){
        return MySQLVersion.SERVER_VERSION_STRING;
    }
}

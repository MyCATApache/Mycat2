package io.mycat.calcite.sqlfunction.stringfunction;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunctionCategory;

public class MycatStringFunction extends MycatSqlDefinedFunction {


    public MycatStringFunction(String name, ScalarFunction function) {
        super(name, function, SqlFunctionCategory.STRING);
    }

}

package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class LocateFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(LocateFunction.class,
            "locate");

    public static final LocateFunction INSTANCE = new LocateFunction();

    public LocateFunction() {
        super(new SqlIdentifier("locate", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.INTEGER),  InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.or( OperandTypes.family(SqlTypeFamily.STRING,  SqlTypeFamily.STRING),
                        OperandTypes.family(SqlTypeFamily.STRING,  SqlTypeFamily.STRING,SqlTypeFamily.INTEGER)
                        ),
                        ImmutableList.of(),
                scalarFunction);
    }

    public static Integer locate(String substr,String str,Integer pos) {
        if (str == null || substr == null||pos == null) {
            return null;
        }
        return str.indexOf(substr.toLowerCase(),pos)+1;
    }
    public static Integer locate(String substr,String str) {
        return locate(substr, str,0);
    }
}
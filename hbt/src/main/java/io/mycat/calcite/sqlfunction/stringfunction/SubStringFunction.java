package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public class SubStringFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(SubStringFunction.class,
            "subString");

    public static final SubStringFunction INSTANCE = new SubStringFunction();

    public SubStringFunction() {
        super(new SqlIdentifier("SUBSTRING", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR),null,
                OperandTypes.VARIADIC,
               null,
                scalarFunction);
    }
    public static String subString(Object... args){
        if (args.length == 3){
            return subString((String) args[0],(Integer)args[1],(Integer)args[2]);
        }else {
            return subString((String) args[0],(Integer)args[1]);
        }
    }
    public static String subString(String str, Integer pos) {
        if (str == null || pos == null) {
            return null;
        }
        pos = (pos < 0) ? str.length() + pos : pos - 1;
        return str.substring(pos);
    }

    public static String subString(String str, Integer pos, Integer len) {
        if (str == null || pos == null || len == null || len <= 0) {
            return null;
        }
        pos = (pos < 0) ? str.length() + pos : pos - 1;
        return str.substring(pos, pos + len);
    }
}
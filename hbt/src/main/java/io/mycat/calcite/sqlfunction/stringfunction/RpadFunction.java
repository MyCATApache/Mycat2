package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class RpadFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(RpadFunction.class,
            "rpad");

    public static final RpadFunction INSTANCE = new RpadFunction();

    public RpadFunction() {
        super(new SqlIdentifier("rpad", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.or(OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
                        OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING)),
                ImmutableList.of(),
                scalarFunction);
    }

    public static String rpad(String str, Integer len, String padstr) {
        if (str == null || len == null || len < 0 ||padstr == null||padstr.isEmpty()) {
            return null;
        }
        if (len<str.length()){
            return str.substring(0,len);
        }
        StringBuilder sb = new StringBuilder(str);
        int count = len - str.length();
        for (int i = 0; i < count; i++) {
            sb.append(padstr);
        }
        return sb.toString();
    }

    public static String rpad(String str, Integer len) {
        return rpad(str, len, " ");
    }
}
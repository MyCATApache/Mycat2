package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class LpadFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(LpadFunction.class,
            "lpad");

    public static final LpadFunction INSTANCE = new LpadFunction();

    public LpadFunction() {
        super(new SqlIdentifier("lpad", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.or(OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
                        OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING)),
                ImmutableList.of(),
                scalarFunction);
    }

    public static String lpad(String str, Integer len, String padstr) {
        if (str == null || len == null || len < 0 ||padstr == null||padstr.isEmpty()) {
            return null;
        }
        if (len<str.length()){
            return str.substring(0,len);
        }
        StringBuilder sb = new StringBuilder();
        int count = len - str.length();
        for (int i = 0; i < count; i++) {
            sb.append(padstr);
        }
        return sb.append(str).toString();
    }

    public static String lpad(String str, Integer len) {
        return lpad(str, len, " ");
    }
}
package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

public class ConcatFunction extends MycatSqlDefinedFunction {
    public static final ConcatFunction INSTANCE = new ConcatFunction();

    public ConcatFunction() {
        super(new SqlIdentifier("concat", SqlParserPos.ZERO),
                ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE, null,
                OperandTypes.repeat(SqlOperandCountRanges.from(2),
                        OperandTypes.STRING), ImmutableList.of(),
                ScalarFunctionImpl.create(ConcatFunction.class, "concat"));
    }

    public static String concat(String... n) {
        StringBuilder sb = new StringBuilder();
        for (String s : n) {
            if (s == null) {
                return null;
            }
            sb.append(s);
        }

        return sb.toString();
    }
}
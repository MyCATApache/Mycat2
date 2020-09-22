package io.mycat.calcite.sqlfunction.stringfunction;

import com.alibaba.fastsql.util.StringUtils;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class QuoteFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(QuoteFunction.class,
            "quote");

    public static final QuoteFunction INSTANCE = new QuoteFunction();

    public QuoteFunction() {
        super(new SqlIdentifier("QUOTE", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static String quote(String str) {
        if (str == null) {
            return null;
        }
       return StringUtils.quoteAlias(str);
    }
}
package io.mycat.calcite.sqlfunction.stringfunction;

import com.alibaba.fastsql.util.StringUtils;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class QuoteFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(QuoteFunction.class,
            "quote");

    public static final QuoteFunction INSTANCE = new QuoteFunction();

    public QuoteFunction() {
        super("QUOTE", scalarFunction);
    }

    public static String quote(String str) {
        if (str == null) {
            return null;
        }
       return StringUtils.quoteAlias(str);
    }
}
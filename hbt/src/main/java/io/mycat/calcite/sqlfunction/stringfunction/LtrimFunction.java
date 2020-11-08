package io.mycat.calcite.sqlfunction.stringfunction;

import com.alibaba.fastsql.util.StringUtils;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class LtrimFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(LtrimFunction.class,
            "ltrim");

    public static final LtrimFunction INSTANCE = new LtrimFunction();

    public LtrimFunction() {
        super("ltrim", scalarFunction);
    }

    public static String ltrim(String str) {
        if (str == null ) {
            return null;
        }
        return StringUtils.ltrim(str);
    }
}
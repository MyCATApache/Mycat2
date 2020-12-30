package io.mycat.calcite.sqlfunction.stringfunction;

import com.alibaba.druid.util.StringUtils;
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

    public static String ltrim(String val) {
        if (val == null) {
            return null;
        }

        int len = val.length();
        int st = 0;
        while ((st < len) && (val.charAt(st) <= ' ')) {
            st++;
        }

        return (st > 0) ? val.substring(st, len) : val;
    }
}
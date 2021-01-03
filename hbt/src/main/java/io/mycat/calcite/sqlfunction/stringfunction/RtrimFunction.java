package io.mycat.calcite.sqlfunction.stringfunction;

import com.alibaba.druid.util.StringUtils;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class RtrimFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(RtrimFunction.class,
            "rtrim");

    public static final RtrimFunction INSTANCE = new RtrimFunction();

    public RtrimFunction() {
        super("rtrim", scalarFunction);
    }

    public static String rtrim(String val) {
        if (val == null) {
            return null;
        }

        int len = val.length();
//        char[] val = new char[value.length()];    /* avoid getfield opcode */

//        while ((st < len) && (val.charAt(st) <= ' ')) {
//            st++;
//        }
        while (val.charAt(len - 1) <= ' ') {
            len--;
        }
        return (len < val.length()) ? val.substring(0, len) : val;
    }
}
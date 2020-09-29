package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

public class ConcatFunction extends MycatStringFunction {

    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ConcatFunction.class,
            "concat");
    public static final ConcatFunction INSTANCE = new ConcatFunction();
    public ConcatFunction() {
        super("concat",  ScalarFunctionImpl.create(ConcatFunction.class,
                "concat"));
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
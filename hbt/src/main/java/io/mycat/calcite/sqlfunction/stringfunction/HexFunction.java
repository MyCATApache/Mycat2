package io.mycat.calcite.sqlfunction.stringfunction;

import com.alibaba.fastsql.util.HexBin;
import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;


public class HexFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(HexFunction.class,
        "hex");

    public static final HexFunction INSTANCE = new HexFunction();

    public HexFunction() {
        super(new SqlIdentifier("hex", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), null,
                OperandTypes.or(OperandTypes.STRING,OperandTypes.INTEGER), ImmutableList.of(),
                scalarFunction);
    }

    public static String hex(Object param0Value) {
        if (param0Value instanceof String) {
            byte[] bytes = ((String) param0Value).getBytes();
            String result = HexBin.encode(bytes);
            return result;
        }

        if (param0Value instanceof Number) {
            long value = ((Number) param0Value).longValue();
            String result = Long.toHexString(value).toUpperCase();
            return result;
        }
        throw new UnsupportedOperationException();
    }
}
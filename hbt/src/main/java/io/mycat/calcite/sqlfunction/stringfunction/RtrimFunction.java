package io.mycat.calcite.sqlfunction.stringfunction;

import com.alibaba.fastsql.util.StringUtils;
import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class RtrimFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(RtrimFunction.class,
            "rtrim");

    public static final RtrimFunction INSTANCE = new RtrimFunction();

    public RtrimFunction() {
        super(new SqlIdentifier("rtrim", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING),
                ImmutableList.of(),
                scalarFunction);
    }

    public static String rtrim(String str) {
        if (str == null ) {
            return null;
        }
        return StringUtils.rtrim(str);
    }
}
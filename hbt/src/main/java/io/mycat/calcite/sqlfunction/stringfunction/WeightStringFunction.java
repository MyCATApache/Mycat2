package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import lombok.SneakyThrows;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.Objects;

public class WeightStringFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(WeightStringFunction.class,
            "weightString");

    public static final WeightStringFunction INSTANCE = new WeightStringFunction();

    public WeightStringFunction() {
        super(new SqlIdentifier("WEIGHT_STRING", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.VARIADIC,
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    @SneakyThrows
    public static String weightString(Object... args) {
        if(Arrays.stream(args).anyMatch(Objects::isNull)){
            return null;
        }
        throw new UnsupportedOperationException("WEIGHT_STRING");
    }
}
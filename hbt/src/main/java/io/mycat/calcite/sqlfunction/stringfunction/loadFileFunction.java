package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import lombok.SneakyThrows;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

import java.nio.file.Files;
import java.nio.file.Paths;

public class loadFileFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(loadFileFunction.class,
            "loadFile");

    public static final loadFileFunction INSTANCE = new loadFileFunction();

    public loadFileFunction() {
        super(new SqlIdentifier("load_file", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    @SneakyThrows
    public static String loadFile(String file) {
        return new String(Files.readAllBytes(Paths.get(file)));
    }
}
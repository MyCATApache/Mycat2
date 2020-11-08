package io.mycat.calcite.sqlfunction.stringfunction;


import lombok.SneakyThrows;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

import java.nio.file.Files;
import java.nio.file.Paths;

public class loadFileFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(loadFileFunction.class,
            "loadFile");

    public static final loadFileFunction INSTANCE = new loadFileFunction();

    public loadFileFunction() {
        super("load_file", scalarFunction);
    }

    @SneakyThrows
    public static String loadFile(String file) {
        return new String(Files.readAllBytes(Paths.get(file)));
    }
}
package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class UncompressedLengthFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UncompressedLengthFunction.class,
            "uncompressedLength");

    public static final UncompressedLengthFunction INSTANCE = new UncompressedLengthFunction();

    public UncompressedLengthFunction() {
        super("UNCOMPRESSED_LENGTH", scalarFunction);
    }

    public static Integer uncompressedLength(String str) {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return 0;
        }
        throw new UnsupportedOperationException("uncompressedLength");
    }
}
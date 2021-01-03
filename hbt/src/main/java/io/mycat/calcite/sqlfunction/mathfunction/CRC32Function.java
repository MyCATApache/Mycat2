package io.mycat.calcite.sqlfunction.mathfunction;

import io.mycat.calcite.MycatScalarFunction;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.*;

import java.util.zip.CRC32;

public class CRC32Function extends MycatSqlDefinedFunction {
    public static final CRC32Function INSTANCE = new CRC32Function();

    public CRC32Function() {
        super("CRC32", ReturnTypes.BIGINT,
                InferTypes.FIRST_KNOWN, OperandTypes.STRING,
                MycatScalarFunction.create(CRC32Function.class,
                        "crc32", 1),
                SqlFunctionCategory.STRING);
    }

    public static Long crc32(String arg0) {
        if (arg0 == null) {
            return null;
        }
        CRC32 crc32 = new CRC32();
        crc32.update(arg0.getBytes());

        return crc32.getValue();
    }
}

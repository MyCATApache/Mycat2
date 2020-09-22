package io.mycat.calcite.sqlfunction.stringfunction;

import com.alibaba.druid.sql.visitor.functions.OneParamFunctions;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class SoundexFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(SoundexFunction.class,
            "soundex");

    public static final SoundexFunction INSTANCE = new SoundexFunction();

    public SoundexFunction() {
        super(new SqlIdentifier("Soundex", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static String soundex(String str) {
        if (str == null) {
            return null;
        }
       return OneParamFunctions.soundex(str);
    }
}
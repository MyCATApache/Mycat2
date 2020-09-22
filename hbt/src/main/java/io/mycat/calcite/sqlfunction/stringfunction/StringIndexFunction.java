package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

import java.util.ArrayList;
import java.util.List;

public class StringIndexFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(StringIndexFunction.class,
            "subStringIndex");

    public static final StringIndexFunction INSTANCE = new StringIndexFunction();

    public StringIndexFunction() {
        super(new SqlIdentifier("SUBSTRING_INDEX", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR), InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
                getRelDataType(scalarFunction),
                scalarFunction);
    }

    public static String subStringIndex(String str, String delim, Integer count) {
        if (str == null || delim == null || count == null) {
            return null;
        }
        if (str.isEmpty() || delim.isEmpty() || count == 0) {
            return "";
        }
        List<String> queue = new ArrayList<>();
        for (; ; ) {
            int index = str.indexOf(delim);
            if (index == 0) {
                str = str.substring(index + delim.length());
            } else if (index == -1) {
                queue.add(str);
                break;
            } else {
                queue.add(str.substring(0, index));
                str = str.substring(index + delim.length());
            }
        }
        boolean reverse = count < 0;
        if (!reverse) {
            count = Math.min(count, queue.size());
            queue = queue.subList(0, count);
            return String.join(delim, queue);
        } else {
            count = -count;
            count = Math.min(count, queue.size());
            return String.join(delim, queue.subList( queue.size() - count,queue.size()));
        }
    }
}
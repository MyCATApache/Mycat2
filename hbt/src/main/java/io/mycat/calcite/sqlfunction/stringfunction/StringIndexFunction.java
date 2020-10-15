package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

import java.util.ArrayList;
import java.util.List;

public class StringIndexFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(StringIndexFunction.class,
            "subStringIndex");

    public static final StringIndexFunction INSTANCE = new StringIndexFunction();

    public StringIndexFunction() {
        super("SUBSTRING_INDEX", scalarFunction);
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
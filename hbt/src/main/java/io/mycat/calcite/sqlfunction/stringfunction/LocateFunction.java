package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class LocateFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(LocateFunction.class,
            "locate");

    public static final LocateFunction INSTANCE = new LocateFunction();

    public LocateFunction() {
        super("locate",scalarFunction);
    }

    public static Integer locate(String substr,String str,Integer pos) {
        if (str == null || substr == null||pos == null) {
            return null;
        }
        return str.indexOf(substr.toLowerCase(),pos)+1;
    }
}
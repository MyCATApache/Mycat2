package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class PositionFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(PositionFunction.class,
            "position");

    public static final PositionFunction INSTANCE = new PositionFunction();

    public PositionFunction() {
        super("position", scalarFunction);
    }

    public static Integer position(String substr,String str) {
      return LocateFunction.locate(substr,str,0);
    }
}
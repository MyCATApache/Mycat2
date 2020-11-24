package io.mycat.calcite.sqlfunction.stringfunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;


public class Locate2Function extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(Locate2Function.class,
            "locate");

    public static final Locate2Function INSTANCE = new Locate2Function();

    public Locate2Function() {
        super("locate",scalarFunction);
    }

    public static Integer locate(String substr,String str) {
        return LocateFunction.locate(substr,str,0);
    }
}
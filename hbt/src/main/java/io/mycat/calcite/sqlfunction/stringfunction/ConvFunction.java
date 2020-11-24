package io.mycat.calcite.sqlfunction.stringfunction;


import io.mycat.calcite.MycatScalarFunction;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;

import java.lang.reflect.Method;

public class ConvFunction extends MycatStringFunction {

    public static final ConvFunction INSTANCE = new ConvFunction();

    public ConvFunction() {
        super("CONV",
                        MycatScalarFunction.create(ConvFunction.class,"conv",3));

    }
//
//    @Override
//    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
//        Method method = Types.lookupMethod(ConvFunction.class,
//                "conv", String.class);
//        return Expressions.call(method, translator.translateList(call.getOperands(), nullAs));
//    }

    public static String conv(String n, Integer from, Integer base) {
        if (n == null || from == null || base == null) {
            return null;
        }
        long dec = 0;

        if (Math.abs(base) > 36 ||
                Math.abs(base) < 2 || Math.abs(from) > 36 || Math.abs(from) < 2 || n.length() == 0) {
            return null;
        }
        if (from < 0) {
            from = -from;
        }
        dec = Long.parseLong(n, from);
        return Long.toString(dec, base);
    }
}
package io.mycat.calcite.sqlfunction.mathfunction;

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.lang.reflect.Method;
import java.util.concurrent.ThreadLocalRandom;

public class RandFunction extends MycatSqlDefinedFunction {
    public static RandFunction INSTANCE = new RandFunction();

    public RandFunction() {
        super("RAND", ReturnTypes.DOUBLE, InferTypes.FIRST_KNOWN, OperandTypes.VARIADIC, null, SqlFunctionCategory.SYSTEM);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        Method rand;
        if (call.getOperands().isEmpty()){
            rand = Types.lookupMethod(RandFunction.class, "rand");
       }else {
            rand = Types.lookupMethod(RandFunction.class, "rand",Number.class);
        }
        return Expressions.call(rand,translator.translateList(call.getOperands(),nullAs));
    }
    public static double rand() {
        return ThreadLocalRandom.current().nextDouble();
    }
    public static Double rand(Number number) {
        return ThreadLocalRandom.current().nextDouble(number.doubleValue());
    }
}

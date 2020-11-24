package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.lang.reflect.Method;
import java.util.ArrayList;

public class ConcatWsFunction extends MycatSqlDefinedFunction {

    public static final ConcatWsFunction INSTANCE = new ConcatWsFunction();

    public ConcatWsFunction() {
        super("concat_ws", ReturnTypes.VARCHAR_2000, InferTypes.VARCHAR_1024, OperandTypes.SAME_VARIADIC, null, SqlFunctionCategory.STRING);
    }
    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        Method method = Types.lookupMethod(ConcatWsFunction.class, "concatWs");
        return Expressions.call(method,translator.translateList(call.getOperands(),nullAs));
    }

    public static String concatWs(String... n) {
        if (n == null || n.length == 0) {
            return null;
        }
        String seq = n[0];
        if (seq == null) return null;
        ArrayList<String> list = new ArrayList<>(n.length);
        for (int i = 1; i < n.length; i++) {
            String s = n[i];
            if (s != null){
                list.add(s);
            }
        }
        return String.join(seq,list);
    }
}
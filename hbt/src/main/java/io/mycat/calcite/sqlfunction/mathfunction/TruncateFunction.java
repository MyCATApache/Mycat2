package io.mycat.calcite.sqlfunction.mathfunction;

import io.mycat.calcite.MycatScalarFunction;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class TruncateFunction extends MycatSqlDefinedFunction {
    public static TruncateFunction INSTANCE = new TruncateFunction();

    public TruncateFunction() {
        super("TRUNCATE",
                ReturnTypes.ARG0_NULLABLE,
                null,
                OperandTypes.NUMERIC_OPTIONAL_INTEGER,

               null, SqlFunctionCategory.SYSTEM);
    }


    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        List<Expression> expressions = translator.translateList(call.getOperands(), nullAs);
        Class[] classes = expressions.stream().map(i -> i.getType()).toArray(n->new Class[n]);
        Method struncate = Types.lookupMethod(SqlFunctions.class, "struncate", classes);
        return Expressions.call(struncate,expressions);
    }

}

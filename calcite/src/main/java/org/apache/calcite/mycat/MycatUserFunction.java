package org.apache.calcite.mycat;

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.util.List;

public class MycatUserFunction extends MycatSqlDefinedFunction {
    public final static MycatUserFunction INSTANCE = new MycatUserFunction();

    public MycatUserFunction() {
        super("USER",
                ReturnTypes.VARCHAR_2000,
                InferTypes.RETURN_TYPE, OperandTypes.NILADIC, null, SqlFunctionCategory.SYSTEM);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        return Expressions.call(Expressions.variable(org.apache.calcite.MycatContext.class,"context"),
                "getUser");
    }
}

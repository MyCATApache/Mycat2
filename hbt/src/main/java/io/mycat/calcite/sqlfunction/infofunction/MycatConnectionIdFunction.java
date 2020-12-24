package io.mycat.calcite.sqlfunction.infofunction;

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class MycatConnectionIdFunction extends MycatSqlDefinedFunction {
    public final static MycatConnectionIdFunction INSTANCE = new MycatConnectionIdFunction();

    public MycatConnectionIdFunction() {
        super("CONNECTION_ID",
                ReturnTypes.BIGINT_NULLABLE,
                InferTypes.RETURN_TYPE, OperandTypes.NILADIC, null, SqlFunctionCategory.SYSTEM);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        return Expressions.call(Expressions.variable(org.apache.calcite.MycatContext.class, "context"), "getConnectionId"
        );
    }
}

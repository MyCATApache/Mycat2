package io.mycat.calcite.sqlfunction.infofunction;

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.util.List;

public class MycatSessionValueFunction extends MycatSqlDefinedFunction {

    public static MycatSessionValueFunction INSTANCE = new MycatSessionValueFunction();

    public MycatSessionValueFunction() {
        super("MYCATSESSIONVALUE", ReturnTypes.ARG0,InferTypes.ANY_NULLABLE, OperandTypes.ANY, null,
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        List<Expression> argValueList = translator.translateList(call.getOperands(),nullAs);
        return Expressions.call(
                NewMycatDataContext.ROOT,
                "getSessionVariable",
                argValueList.get(0));
    }
}


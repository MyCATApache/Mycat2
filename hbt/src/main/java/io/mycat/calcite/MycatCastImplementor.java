package io.mycat.calcite;

import io.mycat.MycatClassResolver;
import io.mycat.calcite.sqlfunction.datefunction.StringToTimestampFunction;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;

public class MycatCastImplementor implements RexImpTable.RexCallImplementor {
    private final RexImpTable.RexCallImplementor rexCallImplementor;

    @SneakyThrows
    public MycatCastImplementor() {
        RexImpTable instance = RexImpTable.INSTANCE;//init
        Map<SqlOperator, RexImpTable.RexCallImplementor> map = MycatClassResolver.forceStaticGet(RexImpTable.class, RexImpTable.INSTANCE, "map");
        this.rexCallImplementor = map.get(SqlStdOperatorTable.CAST);
    }

    @Override
    public RexToLixTranslator.Result implement(RexToLixTranslator translator, RexCall call, List<RexToLixTranslator.Result> arguments) {
        if (call.getOperator() == SqlStdOperatorTable.CAST) {
            if (call.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP) {
                if (call.getOperands().size() == 1) {
                    SqlTypeName valueType = call.getOperands().get(0).getType().getSqlTypeName();
                    if (SqlTypeName.STRING_TYPES.contains(valueType)) {
                        RexImpTable.RexCallImplementor rexCallImplementor = RexImpTable.INSTANCE.get(StringToTimestampFunction.INSTANCE);
                        return rexCallImplementor.implement(translator, call, arguments);
                    }
                }
            }
        }
        return rexCallImplementor.implement(translator, call, arguments);
    }
}
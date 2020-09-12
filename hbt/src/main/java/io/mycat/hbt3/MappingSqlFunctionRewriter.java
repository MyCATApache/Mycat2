package io.mycat.hbt3;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatCatalogReader;
import io.mycat.calcite.MycatScalarFunction;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class MappingSqlFunctionRewriter extends RelShuttleImpl {
    public static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, SqlUserDefinedFunction>>
            dyfunctionCache = new ConcurrentHashMap<>();
    public static final RexShuttle FINDER = new RexShuttle() {
        @Override
        public RexNode visitCall(RexCall call) {
            SqlOperator operator = call.getOperator();
            List<RexNode> operands = call.getOperands();
            int paramSize = operands.size();
            if (operator instanceof SqlUnresolvedFunction) {
                SqlUnresolvedFunction unresolvedFunction = (SqlUnresolvedFunction) operator;
                SqlIdentifier sqlIdentifier = unresolvedFunction.getSqlIdentifier();
                String methodName = unresolvedFunction.getName();
                RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
                JavaTypeFactoryImpl typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;
                ConcurrentHashMap<Integer, SqlUserDefinedFunction> map = dyfunctionCache.computeIfAbsent(methodName, s -> new ConcurrentHashMap<>());
                SqlUserDefinedFunction sqlUserDefinedFunction = map.computeIfAbsent(paramSize, integer -> {
                    Class aClass = MycatCalciteSupport.functions.get(methodName, false);
                    SqlOperandTypeInference operandTypeInference = unresolvedFunction.getOperandTypeInference();
                    SqlOperandTypeChecker operandTypeChecker = unresolvedFunction.getOperandTypeChecker();
                    ScalarFunction scalarFunction = MycatScalarFunction.create(aClass, paramSize);
                    return new SqlUserDefinedFunction(sqlIdentifier,
                            MycatCatalogReader.infer(scalarFunction),
                            operandTypeInference ,
                            operandTypeChecker,
                            scalarFunction.getParameters().stream().map(i->i.getType(MycatCalciteSupport.INSTANCE.TypeFactory)).collect(Collectors.toList()),
                            scalarFunction);

                });
                ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
                for (int i = 0; i < paramSize; i++) {
                    RexNode rexNode = operands.get(i);
                    builder.add(rexBuilder.makeCast(sqlUserDefinedFunction.getParamTypes().get(i), rexNode));
                }
                return rexBuilder.makeCall(sqlUserDefinedFunction, builder.build());
            }
            return super.visitCall(call);
        }
    };

    @Override
    public RelNode visit(LogicalFilter filter) {
        return filter.accept(FINDER);
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return null;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        return project.accept(FINDER);
    }
}
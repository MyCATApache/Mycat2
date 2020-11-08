//package io.mycat.calcite;
//
//import org.apache.calcite.adapter.enumerable.EnumUtils;
//import org.apache.calcite.adapter.enumerable.NullPolicy;
//import org.apache.calcite.adapter.enumerable.RexImpTable;
//import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
//import org.apache.calcite.linq4j.tree.*;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.rex.RexCall;
//import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.sql.SqlOperator;
//import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
//import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
//
//import java.lang.reflect.Modifier;
//import java.lang.reflect.Type;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.stream.Collectors;
//
//public class MycatAbstractRexCallImplementor implements RexImpTable.RexCallImplementor {
//    final NullPolicy nullPolicy;
//    private final boolean harmonize;
//
//    MycatAbstractRexCallImplementor(NullPolicy nullPolicy, boolean harmonize) {
//        this.nullPolicy = nullPolicy;
//        this.harmonize = harmonize;
//    }
//
//    @Override public RexToLixTranslator.Result implement(
//            final RexToLixTranslator translator,
//            final RexCall call,
//            final List<RexToLixTranslator.Result> arguments) {
//        final List<Expression> argIsNullList = new ArrayList<>();
//        final List<Expression> argValueList = new ArrayList<>();
//        for (RexToLixTranslator.Result result: arguments) {
//            argIsNullList.add(result.isNullVariable);
//            argValueList.add(result.valueVariable);
//        }
//        final Expression condition = getCondition(argIsNullList);
//        final ParameterExpression valueVariable =
//                genValueStatement(translator, call, argValueList, condition);
//        final ParameterExpression isNullVariable =
//                genIsNullStatement(translator, valueVariable);
//        return new RexToLixTranslator.Result(isNullVariable, valueVariable);
//    }
//
//    // Variable name facilitates reasoning about issues when necessary
//    abstract String getVariableName();
//
//    /** Figures out conditional expression according to NullPolicy. */
//    Expression getCondition(final List<Expression> argIsNullList) {
//        if (argIsNullList.size() == 0
//                || nullPolicy == null
//                || nullPolicy == NullPolicy.NONE) {
//            return FALSE_EXPR;
//        }
//        if (nullPolicy == NullPolicy.ARG0) {
//            return argIsNullList.get(0);
//        }
//        return Expressions.foldOr(argIsNullList);
//    }
//
//    // E.g., "final Integer xxx_value = (a_isNull || b_isNull) ? null : plus(a, b)"
//    private ParameterExpression genValueStatement(
//            final RexToLixTranslator translator,
//            final RexCall call, final List<Expression> argValueList,
//            final Expression condition) {
//        List<Expression> optimizedArgValueList = argValueList;
//        if (harmonize) {
//            optimizedArgValueList =
//                    harmonize(optimizedArgValueList, translator, call);
//        }
//        optimizedArgValueList = unboxIfNecessary(optimizedArgValueList);
//
//        final Expression callValue =
//                implementSafe(translator, call, optimizedArgValueList);
//
//        // In general, RexCall's type is correct for code generation
//        // and thus we should ensure the consistency.
//        // However, for some special cases (e.g., TableFunction),
//        // the implementation's type is correct, we can't convert it.
//        final SqlOperator op = call.getOperator();
//        final Type returnType = translator.typeFactory.getJavaClass(call.getType());
//        final boolean noConvert = (returnType == null)
//                || (returnType == callValue.getType())
//                || (op instanceof SqlUserDefinedTableMacro)
//                || (op instanceof SqlUserDefinedTableFunction);
//        final Expression convertedCallValue =
//                noConvert
//                        ? callValue
//                        : EnumUtils.convert(callValue, returnType);
//
//        final Expression valueExpression =
//                Expressions.condition(condition,
//                        getIfTrue(convertedCallValue.getType(), argValueList),
//                        convertedCallValue);
//        final ParameterExpression value =
//                Expressions.parameter(convertedCallValue.getType(),
//                        translator.getBlockBuilder().newName(getVariableName() + "_value"));
//        translator.getBlockBuilder().add(
//                Expressions.declare(Modifier.FINAL, value, valueExpression));
//        return value;
//    }
//
//    Expression getIfTrue(Type type, final List<Expression> argValueList) {
//        return getDefaultValue(type);
//    }
//
//    // E.g., "final boolean xxx_isNull = xxx_value == null"
//    private ParameterExpression genIsNullStatement(
//            final RexToLixTranslator translator, final ParameterExpression value) {
//        final ParameterExpression isNullVariable =
//                Expressions.parameter(Boolean.TYPE,
//                        translator.getBlockBuilder().newName(getVariableName() + "_isNull"));
//        final Expression isNullExpression = translator.checkNull(value);
//        translator.getBlockBuilder().add(
//                Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
//        return isNullVariable;
//    }
//
//    /** Ensures that operands have identical type. */
//    private List<Expression> harmonize(final List<Expression> argValueList,
//                                       final RexToLixTranslator translator, final RexCall call) {
//        int nullCount = 0;
//        final List<RelDataType> types = new ArrayList<>();
//        final RelDataTypeFactory typeFactory =
//                translator.builder.getTypeFactory();
//        for (RexNode operand : call.getOperands()) {
//            RelDataType type = operand.getType();
//            type = toSql(typeFactory, type);
//            if (translator.isNullable(operand)) {
//                ++nullCount;
//            } else {
//                type = typeFactory.createTypeWithNullability(type, false);
//            }
//            types.add(type);
//        }
//        if (allSame(types)) {
//            // Operands have the same nullability and type. Return them
//            // unchanged.
//            return argValueList;
//        }
//        final RelDataType type = typeFactory.leastRestrictive(types);
//        if (type == null) {
//            // There is no common type. Presumably this is a binary operator with
//            // asymmetric arguments (e.g. interval / integer) which is not intended
//            // to be harmonized.
//            return argValueList;
//        }
//        assert (nullCount > 0) == type.isNullable();
//        final Type javaClass =
//                translator.typeFactory.getJavaClass(type);
//        final List<Expression> harmonizedArgValues = new ArrayList<>();
//        for (Expression argValue : argValueList) {
//            harmonizedArgValues.add(
//                    EnumUtils.convert(argValue, javaClass));
//        }
//        return harmonizedArgValues;
//    }
//
//    /** Under null check, it is safe to unbox the operands before entering the
//     * implementor. */
//    private List<Expression> unboxIfNecessary(final List<Expression> argValueList) {
//        List<Expression> unboxValueList = argValueList;
//        if (nullPolicy == NullPolicy.STRICT || nullPolicy == NullPolicy.ANY
//                || nullPolicy == NullPolicy.SEMI_STRICT) {
//            unboxValueList = argValueList.stream()
//                    .map(this::unboxExpression)
//                    .collect(Collectors.toList());
//        }
//        if (nullPolicy == NullPolicy.ARG0 && argValueList.size() > 0) {
//            final Expression unboxArg0 = unboxExpression(unboxValueList.get(0));
//            unboxValueList.set(0, unboxArg0);
//        }
//        return unboxValueList;
//    }
//
//    private Expression unboxExpression(final Expression argValue) {
//        Primitive fromBox = Primitive.ofBox(argValue.getType());
//        if (fromBox == null || fromBox == Primitive.VOID) {
//            return argValue;
//        }
//        // Optimization: for "long x";
//        // "Long.valueOf(x)" generates "x"
//        if (argValue instanceof MethodCallExpression) {
//            MethodCallExpression mce = (MethodCallExpression) argValue;
//            if (mce.method.getName().equals("valueOf") && mce.expressions.size() == 1) {
//                Expression originArg = mce.expressions.get(0);
//                if (Primitive.of(originArg.type) == fromBox) {
//                    return originArg;
//                }
//            }
//        }
//        return RexImpTable.NullAs.NOT_POSSIBLE.handle(argValue);
//    }
//
//    abstract Expression implementSafe(RexToLixTranslator translator,
//                                      RexCall call, List<Expression> argValueList);
//}
//}
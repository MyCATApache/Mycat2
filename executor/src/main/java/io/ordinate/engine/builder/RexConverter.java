/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.builder;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.sqlfunction.datefunction.ExtractFunction;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.function.bind.IndexedParameterLinkFunction;
import io.ordinate.engine.function.bind.SessionVariable;
import io.ordinate.engine.function.bind.SessionVariableFunction;
import io.ordinate.engine.function.bind.VariableParameterFunction;
import io.ordinate.engine.function.constant.SymbolConstant;
import io.ordinate.engine.schema.InnerType;
import lombok.Getter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.checkerframework.checker.units.qual.C;
import org.checkerframework.checker.units.qual.Time;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Period;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class RexConverter {
    ExecuteCompiler executeCompiler = new ExecuteCompiler();

    public RexConverter() {

    }

    Map<CorrelationKey, List<VariableParameterFunction>> variableParameterFunctionMap = new HashMap<>();
    Map<Integer, IndexedParameterLinkFunction> indexedParameterLinkFunctionMap = new HashMap<>();
    List<SessionVariable> sessionVariableFunctionMap = new ArrayList<>();

    public Function visitDynamicParam(RexDynamicParam inputParam) {
        IndexedParameterLinkFunction indexedParameterLinkFunction = executeCompiler.newIndexVariable(inputParam.getIndex(), convertColumnType(inputParam.getType()));
        indexedParameterLinkFunctionMap.put(indexedParameterLinkFunction.getVariableIndex(), indexedParameterLinkFunction);
        return indexedParameterLinkFunction.getBase();
    }

    public Function convertRex(RexNode rexNode, Schema schema) {
        if (rexNode instanceof RexDynamicParam) {
            return visitDynamicParam((RexDynamicParam) rexNode);
        } else if (rexNode instanceof RexInputRef) {
            return convertRexInputRefToFunction((RexInputRef) rexNode, schema);
        } else if (rexNode instanceof RexLiteral) {
            return convertToFunction((RexLiteral) rexNode);
        } else if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            if (rexCall.op == ExtractFunction.INSTANCE) {
                RexLiteral rex = (RexLiteral) rexCall.getOperands().get(0);
                TimeUnitRange flag = (TimeUnitRange) rex.getValueAs(TimeUnitRange.class);
                TimeUnit startUnit = flag.startUnit;
                TimeUnit endUnit = flag.endUnit;

                RexNode param = rexCall.getOperands().get(1);
                Function function = convertRex(param, schema);
                Function call = ExecuteCompiler.call(startUnit.name(), function);
                return Objects.requireNonNull(call);
            }
            List<Function> childrens = rexCall.getOperands().stream().map(i -> convertRex(i, schema)).collect(Collectors.toList());
            return convertToFunction(rexCall, childrens);
        } else if (rexNode instanceof RexCorrelVariable) {
            throw new UnsupportedOperationException();
        } else if (rexNode instanceof RexFieldAccess) {
            RexFieldAccess rexFieldAccess = (RexFieldAccess) rexNode;
            RexCorrelVariable rexCorrelVariable = (RexCorrelVariable) rexFieldAccess.getReferenceExpr();
            int index = rexFieldAccess.getField().getIndex();
            CorrelationId id = rexCorrelVariable.id;

            CorrelationKey correlationKey = new CorrelationKey();
            correlationKey.correlationId = id;
            correlationKey.index = index;

            List<VariableParameterFunction> variableParameterFunctions = variableParameterFunctionMap.computeIfAbsent(correlationKey, correlationKey1 -> new ArrayList<>());

            VariableParameterFunction variableParameterFunction = executeCompiler.newCorVariable(convertColumnType(rexCorrelVariable.getType()));
            variableParameterFunctions.add(variableParameterFunction);
            return variableParameterFunction;
        }
        throw new UnsupportedOperationException();
    }

    public Function convertRexInputRefToFunction(RexInputRef rexInputRef, Schema schema) {
        int index = rexInputRef.getIndex();
        return executeCompiler.column(index, schema);
    }

    public Function convertToFunction(RexLiteral literal) {
        final String value;
        if (literal.getValue() instanceof NlsString) {
            final NlsString nlsString = (NlsString) literal.getValue();
            value = nlsString.getValue();
        } else if (literal.getValue() instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) literal.getValue();
            // Special treatment for intervals - VoltDB TIMESTAMP expects value in microseconds
            if (literal.getType() instanceof IntervalSqlType) {
                BigDecimal thousand = BigDecimal.valueOf(1000);
                bd = bd.multiply(thousand);
            }
            value = bd.toPlainString();
        } else if (literal.getValue() instanceof GregorianCalendar) {
            // VoltDB TIMESTAMPS expects time in microseconds
            long time = ((GregorianCalendar) literal.getValue()).getTimeInMillis() * 1000;
            value = Long.toString(time);
        } else if (literal.getType().getSqlTypeName().getName().equals("BINARY")) {
            value = literal.getValue().toString();
        } else if (literal.getValue() == null) {
            value = null;
        } else { // @TODO Catch all
            value = literal.getValue().toString();
        }
        RelDataType type = literal.getType();
        switch (type.getSqlTypeName()) {
            case NULL:
                return executeCompiler.makeNullLiteral();
            case TINYINT:
                return executeCompiler.makeTinyIntLiteral(value);
            case SMALLINT:
                return executeCompiler.makeSmallIntLiteral(value);
            case INTEGER:
                return executeCompiler.makeIntLiteral(value);
            case BIGINT:
                return executeCompiler.makeBigintLiteral(value);
            case REAL:
            case FLOAT:
                return executeCompiler.makeFloatLiteral(value);
            case VARCHAR:
                return executeCompiler.makeVarcharLiteral(value);
            case VARBINARY:
                return executeCompiler.makeVarBinaryLiteral(value);
            case DOUBLE:
                return executeCompiler.makeDoubleLiteral(value);
            case DATE:
                return executeCompiler.makeDateLiteral(value);
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return executeCompiler.makeTimeLiteral(value);
            case TIMESTAMP:
                return executeCompiler.makeDatetimeLiteral(value);
            case DECIMAL:
                return executeCompiler.makeDecimalLiteral(value);
            case BOOLEAN:
                return executeCompiler.makeBooleanLiteral(value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return executeCompiler.makeDatetimeLiteral(value);
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                Period period = literal.getValueAs(Period.class);
                return executeCompiler.makePeriodLiteral(period);
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                Duration duration = literal.getValueAs(Duration.class);
                return executeCompiler.makeTimeLiteral(duration);
            case CHAR:
                return executeCompiler.makeCharLiteral(value);
            case BINARY:
                return executeCompiler.makeBinaryLiteral(value);
            case SYMBOL:
                Comparable value1 = literal.getValue();
                return executeCompiler.makeSymbolLiteral(null);
            case ANY:
            case MULTISET:
            case ARRAY:
            case MAP:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
            default:

        }
        throw new UnsupportedOperationException();
    }

    public Function convertToFunction(RexCall call, List<Function> aeOperands) {
        final Function ae;
        SqlKind kind = call.op.kind;
        String lowerName = kind.lowerName;
        switch (call.op.kind) {
            // Conjunction
            case AND:
                ae = executeCompiler.call(lowerName, aeOperands);
                break;
            case OR:
                if (aeOperands.size() == 2) {
                    // Binary OR
                    ae = executeCompiler.call(lowerName, aeOperands);
                } else {
                    // COMPARE_IN
                    ae = executeCompiler.call("in", aeOperands);
                }
                break;
            // Binary Comparison
            case EQUALS:
                ae = executeCompiler.call("=", aeOperands);
                break;
            case NOT_EQUALS:
                ae = executeCompiler.call("!=", aeOperands);
                break;
            case LESS_THAN:
                ae = executeCompiler.call("<", aeOperands);
                break;
            case GREATER_THAN:
                ae = executeCompiler.call(">", aeOperands);
                break;
            case LESS_THAN_OR_EQUAL:
                ae = executeCompiler.call("<=", aeOperands);
                break;
            case GREATER_THAN_OR_EQUAL:
                ae = executeCompiler.call(">=", aeOperands);
                break;
            case LIKE:
                ae = executeCompiler.call("like", aeOperands);
                break;
//            COMPARE_NOTDISTINCT          (ComparisonExpression.class, 19, "NOT DISTINCT", true),

            // Arthimetic Operators
            case PLUS:
                //todo datetime
                ae = executeCompiler.call("+", aeOperands);
                break;
            case MINUS:
                //todo datetime
                // Check for DATETIME - INTERVAL expression first
                // For whatever reason Calcite treats + and - DATETIME operation differently
                ae = executeCompiler.call("-", aeOperands);
                break;
            case TIMES:
                ae = executeCompiler.call("*", aeOperands);
                break;
            case DIVIDE:
                ae = executeCompiler.call("/", aeOperands);
                break;
            case CAST:
                InnerType targetType = convertColumnType(call.getType());
                ae = executeCompiler.cast(aeOperands.get(0), targetType);
                break;
            case NOT:
                ae = executeCompiler.call("not", aeOperands.get(0));
                break;
            case IS_NULL:
                ae = executeCompiler.call("isNull", aeOperands.get(0));
                break;
            case IS_NOT_NULL:
                ae = executeCompiler.call("isNotNull", aeOperands.get(0));
                break;
            case EXISTS:
                ae = executeCompiler.call("exists", aeOperands.get(0));
                break;
            case CASE:
            case COALESCE:
                ae = executeCompiler.call("case", aeOperands.get(0));
                break;
            case OTHER:
            case OTHER_FUNCTION:
            default:
                String callName = call.op.getName().toUpperCase();
                if (callName.contains("SESSIONVALUE")) {
                    Function function = aeOperands.get(0);
                    StringFunction stringFunction = (StringFunction) function;
                    String name = stringFunction.getString(null).toString();
                    SessionVariableFunction sessionVariableFunction = ExecuteCompiler.newSessionVariable(name);
                    sessionVariableFunctionMap.add(sessionVariableFunction);
                    ae = sessionVariableFunction;
                } else {
                    ae = executeCompiler.call(callName, aeOperands);
                    if (ae == null) {
                        throw new IllegalArgumentException("Unsupported Calcite expression type! " +
                                call.op.kind.toString());
                    }
                    if (ae instanceof SessionVariable) {
                        sessionVariableFunctionMap.add((SessionVariable) ae);
                    }

                }
        }
        Objects.requireNonNull(ae);
        return ae;
    }

    public static List<InnerType> convertColumnTypeList(RelDataType type) {
        ArrayList<InnerType> objects = new ArrayList<>();
        for (RelDataTypeField relDataTypeField : type.getFieldList()) {
            InnerType innerType = convertColumnType(relDataTypeField.getType().getSqlTypeName());
            objects.add(innerType);
        }
        return objects;
    }

    public static InnerType convertColumnType(RelDataType type) {
        SqlTypeName sqlTypeName = type.getSqlTypeName();
        return convertColumnType(sqlTypeName);
    }

    @NotNull
    public static InnerType convertColumnType(SqlTypeName sqlTypeName) {
        switch (sqlTypeName) {
            case BOOLEAN:
                return InnerType.BOOLEAN_TYPE;
            case TINYINT:
                return InnerType.INT8_TYPE;
            case SMALLINT:
                return InnerType.INT16_TYPE;
            case INTEGER:
                return InnerType.INT32_TYPE;
            case BIGINT:
                return InnerType.INT64_TYPE;
            case DOUBLE:
            case DECIMAL:
                return InnerType.DOUBLE_TYPE;
            case REAL:
            case FLOAT:
                return InnerType.FLOAT_TYPE;
            case DATE:
                return InnerType.DATE_TYPE;
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIME:
                return InnerType.TIME_MILLI_TYPE;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return InnerType.DATETIME_MILLI_TYPE;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                throw new UnsupportedOperationException();
            case CHAR:
                return InnerType.CHAR_TYPE;
            case VARCHAR:
                return InnerType.STRING_TYPE;
            case BINARY:
            case VARBINARY:
                return InnerType.BINARY_TYPE;
            case NULL:
                return InnerType.NULL_TYPE;
            case ANY:
            case SYMBOL:
            case MULTISET:
            case ARRAY:
            case MAP:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
            default:
                return InnerType.STRING_TYPE;
        }
    }
}

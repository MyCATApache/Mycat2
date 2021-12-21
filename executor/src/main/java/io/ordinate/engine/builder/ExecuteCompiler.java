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

import io.ordinate.engine.function.*;
import io.ordinate.engine.function.aggregate.avg.AvgAggregateFunction;
import io.ordinate.engine.function.aggregate.count.CountColumnAggregateFunction;
import io.ordinate.engine.function.aggregate.count.CountDistinctDoubleColumnAggregateFunction;
import io.ordinate.engine.function.aggregate.count.CountDistinctLongColumnAggregateFunction;
import io.ordinate.engine.function.constant.*;
import io.ordinate.engine.physicalplan.*;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.function.bind.*;
import io.ordinate.engine.function.column.*;
import io.ordinate.engine.function.aggregate.AccumulatorFunction;
import io.ordinate.engine.function.aggregate.*;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.vector.VectorExpression;
import io.ordinate.engine.function.aggregate.any.AnyValueAccumulator;
import io.ordinate.engine.vector.ExprVectorExpression;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.JoinType;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import java.util.*;

public class ExecuteCompiler {
    final static FunctionFactoryCache functionFactoryCache = new FunctionFactoryCache();

    public static Function column(int index, Schema schema) {
        ArrowType type = schema.getFields().get(index).getType();
        switch (InnerType.from(type)) {
            case BOOLEAN_TYPE:
                return BooleanColumn.newInstance(index);
            case INT8_TYPE:
                return ByteColumn.newInstance(index);
            case INT16_TYPE:
                return ShortColumn.newInstance(index);
            case CHAR_TYPE:
                break;
            case INT32_TYPE:
                return IntColumn.newInstance(index);
            case INT64_TYPE:
                return LongColumn.newInstance(index);
            case FLOAT_TYPE:
                return FloatColumn.newInstance(index);
            case DOUBLE_TYPE:
                return DoubleColumn.newInstance(index);
            case DECIMAL_TYPE:
                break;
            case STRING_TYPE:
                return StringColumn.newInstance(index);
            case BINARY_TYPE:
                return BinaryColumn.newInstance(index);
            case UINT8_TYPE:
            case UINT16_TYPE:
            case UINT32_TYPE:
            case UINT64_TYPE:
                break;
            case TIME_MILLI_TYPE:
                return TimeColumn.newInstance(index);
            case DATE_TYPE:
                return DateColumn.newInstance(index);
            case DATETIME_MILLI_TYPE:
                return DatetimeColumn.newInstance(index);
            case SYMBOL_TYPE:
                break;
            case OBJECT_TYPE:
                break;
            case NULL_TYPE:
                break;
        }
        throw new UnsupportedOperationException();
    }

    public static Function call(String name, Function... argExprs) {
        return call(name, Arrays.asList(argExprs));
    }

    public static Function call(String name, List<Function> argExprs) {
        List<FunctionFactoryDescriptor> functions = functionFactoryCache.getListByName(name);
        if (functions.isEmpty()) {
            return null;
        }
        if (functions.size() == 1) {
            FunctionFactoryDescriptor functionFactoryDescriptor = functions.get(0);
            FunctionFactory factory = functionFactoryDescriptor.getFactory();
            int index = 0;
            List<Function> targetFunctions = new ArrayList<>(functions.size());
            if (argExprs.size() > functionFactoryDescriptor.getArgTypes().size()) {
                targetFunctions.addAll(argExprs);
            } else {
                for (InnerType argType : functionFactoryDescriptor.getArgTypes()) {
                    targetFunctions.add(cast(argExprs.get(index), argType));
                    index++;
                }
            }
            return factory.newInstance(targetFunctions);
        }
        boolean match = false;
        FunctionFactoryDescriptor matchVExpression = null;
        for (FunctionFactoryDescriptor function : functions) {
            List<InnerType> targetTypes = function.getArgTypes();
            int size = targetTypes.size();
            if (size != argExprs.size()) {
                continue;
            }
            boolean argMatch = true;
            for (int i = 0; i < size; i++) {
                argMatch = argMatch && targetTypes.get(i).equals(argExprs.get(i).getType());
            }
            if (argMatch) {
                match = true;
                matchVExpression = function;
                break;
            }
        }
        if (match) {
            return matchVExpression.getFactory().newInstance(argExprs);
        }
        /////////////////////////////////////////////////////////////////////////////////////////////////


        int lowestCastScore = Integer.MAX_VALUE;
        FunctionFactoryDescriptor bestVExpression = null;

        functionBreak:
        for (FunctionFactoryDescriptor function : functions) {
            List<InnerType> targetTypes = function.getArgTypes();
            int size = targetTypes.size();
            if (size != argExprs.size()) {
                continue;
            }
            int score = 0;
            for (int i = 0; i < size; i++) {
                InnerType targetArrowType = targetTypes.get(i);
                InnerType argArrowType = argExprs.get(i).getType();
                if (!targetArrowType.equals(argArrowType)) {
                    if (canCast(argArrowType, targetArrowType)) {
                        score++;
                    } else {
                        break functionBreak;
                    }
                }
            }
            if (score < lowestCastScore) {
                lowestCastScore = score;
                bestVExpression = function;
            }
        }
        if (bestVExpression != null) {
            Function[] newArgExprs = new Function[argExprs.size()];
            List<InnerType> targetTypes = bestVExpression.getArgTypes();
            int size = targetTypes.size();
            for (int i = 0; i < size; i++) {
                InnerType targetArrowType = targetTypes.get(i);
                InnerType argArrowType = argExprs.get(i).getType();
                if (!targetArrowType.equals(argArrowType)) {
                    if (canCast(argArrowType, targetArrowType)) {
                        newArgExprs[i] = cast(argExprs.get(i), targetArrowType);
                    } else {
                        newArgExprs[i] = argExprs.get(i);
                    }
                } else {
                    newArgExprs[i] = argExprs.get(i);
                }
            }
            return call(name, Arrays.asList(newArgExprs));
        }
        //对于变参函数,使用函数原型扩展参数类型到新的长度,然后再匹配
        throw new UnsupportedOperationException();
    }

    private static boolean canCast(InnerType argArrowType, InnerType targetArrowType) {
        return true;
    }

    public static Function cast(Function argExpr, InnerType targetArrowType) {
        InnerType type = argExpr.getType();
        if (type.equals(targetArrowType)) {
            return argExpr;
        }
        List<FunctionFactoryDescriptor> cast = functionFactoryCache.getListByName("cast");
        for (FunctionFactoryDescriptor functionFactoryDescriptor : cast) {
            List<InnerType> argTypes = functionFactoryDescriptor.getArgTypes();
            if (argTypes.get(0).equals(type)) {
                if (targetArrowType.equals(functionFactoryDescriptor.getType())) {
                    return functionFactoryDescriptor.getFactory().newInstance(Collections.singletonList(argExpr));
                }
            }
        }

        throw new UnsupportedOperationException();
    }

    public static RootContext createRootContext() {
        return new RootContext(null);
    }

    public static VariableParameterFunction newCorVariable(InnerType innerType) {
        Function function = newBindVariable(innerType);
        return new VariableParameterFunction(function);
    }

    public static SessionVariableFunction newSessionVariable(String sessionValueName) {
        return new SessionVariableFunction(sessionValueName);
    }

    public static IndexedParameterLinkFunction newIndexVariable(int index, InnerType innerType) {
        Function function = newBindVariable(innerType);
        return new IndexedParameterLinkFunction(index, function);
    }

    public static Function newBindVariable(InnerType innerType) {
        Function res = null;
        switch (innerType) {
            case BOOLEAN_TYPE:
                res = new BooleanBindVariable();
                break;
            case INT8_TYPE:
                res = new ByteBindVariable();
                break;
            case INT16_TYPE:
                res = new ShortBindVariable();
                break;
            case CHAR_TYPE:
                res = new CharBindVariable();
                break;
            case INT32_TYPE:
                res = new IntBindVariable();
                break;
            case INT64_TYPE:
                res = new LongBindVariable();
                break;
            case FLOAT_TYPE:
                res = new FloatBindVariable();
                break;
            case DOUBLE_TYPE:
                res = new DoubleBindVariable();
                break;
            case STRING_TYPE:
                res = new StringBindVariable();
                break;
            case BINARY_TYPE:
                res = new BinaryBindVariable();
                break;
            case UINT8_TYPE:
                res = new ByteBindVariable();
                break;
            case UINT16_TYPE:
                res = new ShortBindVariable();
                break;
            case UINT32_TYPE:
                res = new IntBindVariable();
                break;
            case UINT64_TYPE:
                res = new LongBindVariable();
                break;
            case TIME_MILLI_TYPE:
                res = new TimeBindVariable();
                break;
            case DATE_TYPE:
                res = new DateBindVariable();
                break;
            case DATETIME_MILLI_TYPE:
                res = new TimestampBindVariable();
                break;
            case SYMBOL_TYPE:
                res = new SymbolBindVariable();
                break;
            case OBJECT_TYPE:
            case NULL_TYPE:
                throw new IllegalArgumentException();
        }
        return res;
    }

    public static Function makeNullLiteral() {
        return NullConstant.NULL;
    }

    public static Function makeTinyIntLiteral(Object value) {
        if (value == null) {
            return NullConstant.create(InnerType.INT8_TYPE);
        }
        return ByteConstant.newInstance((byte) value);
    }

    public static Function makeSmallIntLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.INT16_TYPE);
        }
        return ShortConstant.newInstance(Short.parseShort(value));
    }

    public static Function makeIntLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.INT32_TYPE);
        }
        return IntConstant.newInstance(Integer.parseInt(value));
    }

    public static Function makeBigintLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.INT64_TYPE);
        }
        return LongConstant.newInstance(Long.parseLong(value));
    }

    public static Function makeFloatLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.FLOAT_TYPE);
        }
        return FloatConstant.newInstance(Float.parseFloat(value));
    }

    public static Function makeVarcharLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.STRING_TYPE);
        }
        return StringConstant.newInstance(value);
    }

    public static Function makeVarBinaryLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.BINARY_TYPE);
        }
        return BinaryConstant.newInstance(value);
    }

    public static Function makeDatetimeLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.DATETIME_MILLI_TYPE);
        }
        Timestamp timestamp = Timestamp.valueOf(value);
        long time = timestamp.getTime();
        return DatetimeConstant.newInstance(time);
    }

    public static Function makeDoubleLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.DOUBLE_TYPE);
        }
        return DoubleConstant.newInstance(Double.parseDouble(value));
    }

    public static Function makeDateLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.DATE_TYPE);
        }
        return DateConstant.newInstance(Date.parse(value));
    }

    public static Function makeTimeLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.TIME_MILLI_TYPE);
        }
        return TimeConstant.newInstance(Time.parse(value));
    }

    public static Function makeDecimalLiteral(String value) {
        return makeDoubleLiteral(value);
    }

    public static Function makeBooleanLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.BOOLEAN_TYPE);
        }
        return BooleanConstant.newInstance("1".equals(value) || Boolean.parseBoolean(value));
    }

    public static Function makeCharLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.CHAR_TYPE);
        }
        if (value.length() > 1) {
            return StringConstant.newInstance(value);
        } else {
            return CharConstant.newInstance(value.charAt(0));
        }
    }

    public static Function makeBinaryLiteral(String value) {
        return makeVarBinaryLiteral(value);
    }

    public static UnionPlan unionAll(boolean all, List<PhysicalPlan> inputs) {
        return UnionPlan.create(inputs.toArray(new PhysicalPlan[]{}));
    }

    public static Function makeLiteral(int value) {
        return IntConstant.newInstance((value));
    }

    public static Function makeLiteral(boolean value) {
        return BooleanConstant.newInstance(value);
    }

    public Function makePeriodLiteral(Period period) {
        return PeriodConstant.of(period);
    }

    public Function makeTimeLiteral(Duration duration) {
        return TimeConstant.newInstance(duration.toMillis());
    }

    public Function makeSymbolLiteral(String value) {
        return null;
    }


    public static enum JoinImpl {
        HASH, NL
    }

    public static enum AggImpl {
        HASH, MERGE
    }

    public static Schema createJoinSchema(PhysicalPlan left, PhysicalPlan right) {
        Schema leftSchema = left.schema();
        Schema rightSchema = right.schema();


        List<Field> leftFields = leftSchema.getFields();
        List<Field> rightFields = rightSchema.getFields();


        ArrayList<Field> joinFieldList = new ArrayList<>();
        joinFieldList.addAll(leftFields);
        joinFieldList.addAll(rightFields);

        Schema joinSchema = new Schema(joinFieldList);
        return joinSchema;
    }

    public static PhysicalPlan crossJoin(PhysicalPlan left, PhysicalPlan right, JoinType joinType, JoinImpl joinImplType, Function on) {
        return new NLJoinPlan(left, right, joinType, on, createJoinSchema(left, right));
    }

    public static PhysicalPlan project(PhysicalPlan input, Function... exprs) {
        return project(input, Arrays.asList(exprs));
    }


    public static PhysicalPlan castAggProject(PhysicalPlan input, Map<Integer, ArrowType> aggIndexes) {
        PhysicalPlan peek = input;
        Schema schema = peek.schema();
        List<Field> fields = schema.getFields();

        boolean needCastProject = false;
        ArrayList<Function> castFunctions = new ArrayList<>(fields.size());
        for (int columnIndex = 0; columnIndex < peek.schema().getFields().size(); columnIndex++) {
            ArrowType inputType = fields.get(columnIndex).getType();
            ArrowType outputType = aggIndexes.getOrDefault(columnIndex, inputType);
            if (!inputType.equals(outputType)) {
                needCastProject = true;
                String s = "cast" + "(" + InnerType.from(inputType).getAlias() + "):" + InnerType.from(outputType).getAlias();
                Optional<FunctionFactoryDescriptor> functionFactoryDescriptor =
                        functionFactoryCache.getListFullName(
                                s
                        );
                Function function = functionFactoryDescriptor.get().getFactory().newInstance(
                        Collections.singletonList(column(columnIndex, schema)));
                castFunctions.add(function);
            } else {
                castFunctions.add(column(columnIndex, schema));
            }
        }
        if (needCastProject) {
            return project(peek, castFunctions);
        }
        return input;
    }


    public static CountAggregateFunction count() {
        return new CountAggregateFunction();
    }

    public AccumulatorFunction count(int index) {
        return new CountColumnAggregateFunction(index, null);
    }

    public AccumulatorFunction sum(PhysicalPlan input, int index) {
        PhysicalPlan peek = input;
        Field field = peek.schema().getFields().get(index);
        ArrowType outputType = InnerType.castToAggType(field.getType());
        switch (InnerType.from(outputType)) {
            case INT32_TYPE:
            case INT64_TYPE:
                return new SumLongAggregateFunction(index);
            case DOUBLE_TYPE:
            default:
                return new SumDoubleAggregateFunction(index);
        }
    }

    public AccumulatorFunction avg(int index) {
        return new AvgAggregateFunction(index);
    }

    public AccumulatorFunction min(PhysicalPlan input, int index) {
        PhysicalPlan peek = input;
        Field field = peek.schema().getFields().get(index);
        ArrowType outputType = InnerType.castToAggType(field.getType());
        switch (InnerType.from(outputType)) {
            case INT32_TYPE:
            case INT64_TYPE:
                return new MinLongAggregateFunction(index);
            case DOUBLE_TYPE:
            default:
                return new MinDoubleAggregateFunction(index);
        }
    }

    public AccumulatorFunction max(PhysicalPlan input, int index) {
        PhysicalPlan peek = input;
        Field field = peek.schema().getFields().get(index);
        ArrowType outputType = InnerType.castToAggType(field.getType());
        switch (InnerType.from(outputType)) {
            case INT32_TYPE:
            case INT64_TYPE:
                return new MaxLongAggregateFunction(index);
            case DOUBLE_TYPE:
            default:
                return new MaxDoubleAggregateByFunction(index);
        }
    }

    public AccumulatorFunction countDistinct(PhysicalPlan input, int index) {
        PhysicalPlan peek = input;
        Field field = peek.schema().getFields().get(index);
        ArrowType outputType = InnerType.castToAggType(field.getType());
        switch (InnerType.from(outputType)) {
            case INT32_TYPE:
            case INT64_TYPE:
                return new CountDistinctLongColumnAggregateFunction(index);
            case DOUBLE_TYPE:
            default:
                return new CountDistinctDoubleColumnAggregateFunction(index);
        }
    }


    public static PhysicalPlan project(PhysicalPlan input, List<Function> exprs) {
        ArrowType[] exprTypes = new ArrowType[exprs.size()];
        VectorExpression[] expressions = new VectorExpression[exprs.size()];
        for (int i = 0; i < exprTypes.length; i++) {
            Function function = exprs.get(i);
            boolean constant = function.isConstant();
            boolean runtimeConstant = function.isRuntimeConstant();
            exprTypes[i] = function.getType().getArrowType();
            expressions[i] = new ExprVectorExpression(function);
        }
        Schema schema = SchemaBuilder.ofArrowType(exprTypes).toArrow();

        return new ProjectionPlan(input, Arrays.asList(expressions), schema);
    }

    public static TableFunctionPlan values(Function[] functions) {
        return (TableFunctionPlan.create(functions));
    }

    public static TableFunctionListPlan values(ArrayList<Function[]> toArrow, Schema schema) {
        return (TableFunctionListPlan.create(toArrow,schema));
    }

    public static FilterPlan filter(PhysicalPlan input, Function function) {
        return new FilterPlan(input, function, input.schema());
    }

    public static GroupByKeyPlan agg(PhysicalPlan input,GroupKeys... groupKeyList) {
        GroupByKeyPlan groupByKey = new GroupByKeyPlan(input, groupKeyList, input.schema());
        return groupByKey;
    }


    public static PhysicalPlan distinct(PhysicalPlan input, AggImpl impl) {
        int size = input.schema().getFields().size();
        int[] ints = new int[size];
        Arrays.setAll(ints, operand -> operand);

        GroupKeys groupKeys = new GroupKeys(ints);
        switch (impl) {
            case HASH:
                DistinctPlan distinct = new DistinctPlan(input, ints);
                return distinct;
            case MERGE:
                throw new UnsupportedOperationException();
        }
        return null;
    }

    public static AccumulatorFunction anyValue(PhysicalPlan input, int columnIndex) {
        Field field = input.schema().getFields().get(columnIndex);
        return new AnyValueAccumulator(InnerType.from(field.getType()), columnIndex);
    }


    public static PhysicalPlan agg(PhysicalPlan input, AggImpl impl, List<GroupKeys> groupKeyList, List<AccumulatorFunction> groupExprs) {
        PhysicalPlan physicalPlan = null;


        ArrayList<ArrowType> objects = new ArrayList<>();
        for (AccumulatorFunction groupExpr : groupExprs) {
            ArrowType arrowType = groupExpr.getType().getArrowType();
            objects.add(arrowType);
        }
        Schema outputSchema =     SchemaBuilder.ofArrowType(objects).toArrow();

        // AggregateVectorExpression[] aggregateVectorExpressions = getAggregateVectorExpressions(groupExprs);
        if (groupKeyList.isEmpty() && groupExprs.stream().allMatch(i -> i.isVector())) {
            physicalPlan = NoKeysAggPlan.create(outputSchema, input, groupExprs.toArray(new AccumulatorFunction[]{}));
        } else {
            switch (impl) {
                case HASH:
                    physicalPlan = new GroupByKeyWithAggPlan(input, groupKeyList.toArray(new GroupKeys[]{}), groupExprs.toArray(new AccumulatorFunction[]{}), outputSchema);
                    break;
                case MERGE:
                    break;
            }
        }
        return physicalPlan;
    }


}

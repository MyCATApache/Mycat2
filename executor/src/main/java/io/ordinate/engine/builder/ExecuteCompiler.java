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
import io.ordinate.engine.function.constant.*;
import io.ordinate.engine.physicalplan.*;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.function.bind.*;
import io.ordinate.engine.function.column.*;
import io.ordinate.engine.function.aggregate.AccumulatorFunction;
import io.ordinate.engine.function.aggregate.*;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.vector.VectorExpression;
import io.ordinate.engine.function.aggregate.AnyValueAccumulator;
import io.ordinate.engine.vector.ExprVectorExpression;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.JoinType;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

public class ExecuteCompiler {
    int ids = 0;
    FunctionFactoryCache functionFactoryCache = new FunctionFactoryCache();
    LinkedList<PhysicalPlan> stack = new LinkedList<>();
    ArrayList<Function> corMap = new ArrayList<>();
    ArrayList<Function> paramMap = new ArrayList<>();
    private int corIds = 0;

    int nextId() {
        return ids++;
    }

    public Function column(int index) {
        return column(index, stack.peek().schema());
    }

    public Function column(int index, Schema schema) {
        ArrowType type = schema.getFields().get(index).getType();
        switch (InnerType.from(type)) {
            case BOOLEAN_TYPE:
                return BooleanColumn.newInstance(index);
            case INT8_TYPE:
                return ByteColumn.newInstance(index);
            case INT16_TYPE:
                return ShortColumn.newInstance(index);
            case INT32_TYPE:
                return IntColumn.newInstance(index);
            case INT64_TYPE:
                return LongColumn.newInstance(index);
            case FLOAT_TYPE:
                return FloatColumn.newInstance(index);
            case DOUBLE_TYPE:
                return DoubleColumn.newInstance(index);
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
        }
        throw new UnsupportedOperationException();
    }

    public Function call(String name, Function... argExprs) {
        return call(name, Arrays.asList(argExprs));
    }

    public Function call(String name, List<Function> argExprs) {
        List<FunctionFactoryDescriptor> functions = functionFactoryCache.getListByName(name);
        if (functions.isEmpty()) return null;
        if (functions.size() == 1) {
            FunctionFactoryDescriptor functionFactoryDescriptor = functions.get(0);
            FunctionFactory factory = functionFactoryDescriptor.getFactory();
            int index = 0;
            List<Function> targetFunctions = new ArrayList<>(functions.size());
            for (InnerType argType : functionFactoryDescriptor.getArgTypes()) {
                targetFunctions.add(cast(argExprs.get(index), argType));
                index++;
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
                }
            }
            call(name, Arrays.asList(newArgExprs));
        }
        //对于变参函数,使用函数原型扩展参数类型到新的长度,然后再匹配
        throw new UnsupportedOperationException();
    }

    private static boolean canCast(InnerType argArrowType, InnerType targetArrowType) {
        return true;
    }

    public Function cast(Function argExpr, InnerType targetArrowType) {
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

    public RootContext createRootContext() {
        return new RootContext();
    }

    public ExecuteCompiler limit(IntFunction intBindVariable, IntFunction intBindVariable1) {
        PhysicalPlan input = stack.pop();
        stack.push(new LimitPlan(input, intBindVariable, intBindVariable1));
        return this;
    }

    public ExecuteCompiler sort(List<PhysicalSortProperty> physicalSortProperties) {
        PhysicalPlan input = stack.pop();
        stack.push(new SortPlan(input, physicalSortProperties));
        return this;
    }

    public VariableParameterFunction newCorVariable(InnerType innerType) {
        Function function = newBindVariable(innerType);
        return new VariableParameterFunction(function);
    }

    public IndexedParameterLinkFunction newIndexVariable(int index, InnerType innerType) {
        Function function = newBindVariable(innerType);
        return new IndexedParameterLinkFunction(index, function);
    }

    public Function newBindVariable(InnerType innerType) {
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

    public ExecuteCompiler correlate(JoinType joinType, Map<Integer, List<VariableParameterFunction>> variableParameterFunctionMap) {
        PhysicalPlan right = stack.pop();
        PhysicalPlan left = stack.pop();
        CorrelateJoinPlan correlateJoin = new CorrelateJoinPlan(left,right, joinType, variableParameterFunctionMap);
        stack.push(correlateJoin);
        return this;
    }

    public Function makeNullLiteral() {
        return NullConstant.NULL;
    }

    public Function makeTinyIntLiteral(Object value) {
        if (value == null) {
            return NullConstant.create(InnerType.INT8_TYPE);
        }
        return ByteConstant.newInstance((byte) value);
    }

    public Function makeSmallIntLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.INT16_TYPE);
        }
        return ShortConstant.newInstance(Short.parseShort(value));
    }

    public Function makeIntLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.INT32_TYPE);
        }
        return IntConstant.newInstance(Integer.parseInt(value));
    }

    public Function makeBigintLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.INT64_TYPE);
        }
        return LongConstant.newInstance(Long.parseLong(value));
    }

    public Function makeFloatLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.FLOAT_TYPE);
        }
        return FloatConstant.newInstance(Float.parseFloat(value));
    }

    public Function makeVarcharLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.STRING_TYPE);
        }
        return StringConstant.newInstance(value);
    }

    public Function makeVarBinaryLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.BINARY_TYPE);
        }
        return BinaryConstant.newInstance(value);
    }

    public Function makeDatetimeLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.DATETIME_MILLI_TYPE);
        }
        Timestamp timestamp = Timestamp.valueOf(value);
        long time = timestamp.getTime();
        return DatetimeConstant.newInstance(time);
    }

    public Function makeDoubleLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.DOUBLE_TYPE);
        }
        return DoubleConstant.newInstance(Double.parseDouble(value));
    }

    public Function makeDateLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.DATE_TYPE);
        }
        return DateConstant.newInstance( Date.parse(value));
    }

    public Function makeTimeLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.TIME_MILLI_TYPE);
        }
        return TimeConstant.newInstance( Time.parse(value));
    }

    public Function makeDecimalLiteral(String value) {
        return makeDoubleLiteral(value);
    }

    public Function makeBooleanLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.BOOLEAN_TYPE);
        }
        return BooleanConstant.newInstance( "1".equals(value)||Boolean.parseBoolean(value));
    }

    public Function makeCharLiteral(String value) {
        if (value == null) {
            return NullConstant.create(InnerType.CHAR_TYPE);
        }
        return CharConstant.newInstance( value.charAt(0));
    }

    public Function makeBinaryLiteral(String value) {
        return makeVarBinaryLiteral(value);
    }

    public ExecuteCompiler unionAll(boolean all,int size) {
        ArrayList<PhysicalPlan> physicalPlans = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            physicalPlans.add( stack.pop());
        }
        if (all){

        }else {
            throw new UnsupportedOperationException();
        }
        Collections.reverse(physicalPlans);
        stack.push(UnionPlan.create(physicalPlans.toArray(new PhysicalPlan[]{})));
        return this;
    }


    public static enum JoinImpl {
        HASH, NL
    }

    public static enum AggImpl {
        HASH, MERGE
    }

    public ExecuteCompiler startJoin() {
        PhysicalPlan right = stack.pop();
        PhysicalPlan left = stack.pop();
        Schema leftSchema = left.schema();
        Schema rightSchema = right.schema();


        List<Field> leftFields = leftSchema.getFields();
        List<Field> rightFields = rightSchema.getFields();


        ArrayList<Field> joinFieldList = new ArrayList<>();
        joinFieldList.addAll(leftFields);
        joinFieldList.addAll(rightFields);

        Schema joinSchema = new Schema(joinFieldList);
        stack.push(new PhysicalPlan() {
            @Override
            public Schema schema() {
                return joinSchema;
            }

            @Override
            public List<PhysicalPlan> children() {
                return Arrays.asList(left, right);
            }

            @Override
            public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
                return null;
            }

            @Override
            public void accept(PhysicalPlanVisitor physicalPlanVisitor) {

            }
        });
        return this;
    }

    public ExecuteCompiler crossJoin(JoinType joinType, JoinImpl joinImplType, Function on) {
        PhysicalPlan input = stack.pop();
        List<PhysicalPlan> children = input.children();
        NLJoinPlan nlJoin = new NLJoinPlan(children.get(0), children.get(1), joinType, on, input.schema());
        stack.push(nlJoin);
        return this;
    }

    public ExecuteCompiler project(Function... exprs) {
        return project(Arrays.asList(exprs));
    }


    public ExecuteCompiler castAggProject(Map<Integer, ArrowType> aggIndexes) {
        PhysicalPlan peek = stack.peek();
        Schema schema = peek.schema();
        List<Field> fields = schema.getFields();

        boolean needCastProject = false;
        ArrayList<Function> castFunctions = new ArrayList<>(fields.size());
        for (int columnIndex = 0; columnIndex < fields.size(); columnIndex++) {
            ArrowType inputType = fields.get(columnIndex).getType();
            ArrowType outputType = aggIndexes.get(columnIndex);
            if (!inputType.equals(outputType)) {
                needCastProject = true;
                String s = "cast" + "(" + InnerType.from(inputType).getAlias() + "):" + InnerType.from(outputType).getAlias();
                Optional<FunctionFactoryDescriptor> functionFactoryDescriptor =
                        functionFactoryCache.getListFullName(
                                s
                        );
                Function function = functionFactoryDescriptor.get().getFactory().newInstance(
                        Collections.singletonList(column(columnIndex)));
                castFunctions.add(function);
            } else {
                castFunctions.add(column(columnIndex));
            }
        }
        if (needCastProject) {
            return project(castFunctions);
        }
        return this;
    }


    public static CountAggregateFunction count() {
        return new CountAggregateFunction();
    }

    public AccumulatorFunction count(int index) {
        return new CountColumnAggregateFunction(index);
    }

    public AccumulatorFunction sum(int index) {
        PhysicalPlan peek = stack.peek();
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

    public AccumulatorFunction min(int index) {
        PhysicalPlan peek = stack.peek();
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

    public AccumulatorFunction max(int index) {
        PhysicalPlan peek = stack.peek();
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

    public AccumulatorFunction countDistinct(int index) {
        PhysicalPlan peek = stack.peek();
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

    public ExecuteCompiler project(List<Function> exprs) {
        PhysicalPlan project = project(stack.peek(), exprs);
        stack.pop();
        stack.push(project);
        return this;
    }

    public PhysicalPlan project(PhysicalPlan input, List<Function> exprs) {
        ArrowType[] exprTypes = new ArrowType[exprs.size()];
        VectorExpression[] expressions = new VectorExpression[exprs.size()];
        for (int i = 0; i < exprTypes.length; i++) {
            Function function = exprs.get(i);
            boolean constant = function.isConstant();
            boolean runtimeConstant = function.isRuntimeConstant();
            exprTypes[i] = function.getType().getArrowType();
            expressions[i] = new ExprVectorExpression(function, nextId());
        }
        Schema schema = SchemaBuilder.ofArrowType(exprTypes).toArrow();

        return new ProjectionPlan(input, Arrays.asList(expressions), schema);
    }
    public ExecuteCompiler values(Function[] functions) {
        stack.push(TableFunctionPlan.create(functions));
        return this;
    }
    public ExecuteCompiler values(List<Function[]> functions) {
        stack.push(TableFunctionListPlan.create(functions));
        return this;
    }
    public ExecuteCompiler values(Schema toArrow, List<Object[]> rowList) {
        ValuesPlan values = new ValuesPlan(toArrow, rowList);
        stack.push(values);
        return this;
    }

    public ExecuteCompiler filter(Function function) {
        PhysicalPlan peek = stack.peek();
        FilterPlan filter = new FilterPlan(peek, function, peek.schema());
        stack.pop();
        stack.push(filter);
        return this;
    }

    public ExecuteCompiler agg(AggImpl impl, GroupKeys... groupKeyList) {
        GroupByKeyPlan groupByKey = new GroupByKeyPlan(stack.peek(), groupKeyList, stack.peek().schema());
        stack.pop();
        stack.push(groupByKey);
        return this;
    }

    public ExecuteCompiler agg(List<GroupKeys> groupKeyList, List<AccumulatorFunction> groupExprs) {
        return agg(AggImpl.HASH, groupKeyList, groupExprs);
    }

    public ExecuteCompiler distinct(AggImpl impl) {
        PhysicalPlan input = stack.peek();
        int size = input.schema().getFields().size();
        int[] ints = new int[size];
        Arrays.setAll(ints, operand -> operand);

        GroupKeys groupKeys = new GroupKeys(ints);
        stack.pop();
        switch (impl) {
            case HASH:
                DistinctPlan distinct = new DistinctPlan(input, ints);
                stack.push(distinct);
                break;
            case MERGE:
                throw new UnsupportedOperationException();
        }
        return this;
    }

    public AccumulatorFunction anyValue(int columnIndex) {
        PhysicalPlan input = stack.peek();
        Field field = input.schema().getFields().get(columnIndex);
        return new AnyValueAccumulator(InnerType.from(field.getType()), columnIndex);
    }


    public ExecuteCompiler agg(AggImpl impl, List<GroupKeys> groupKeyList, List<AccumulatorFunction> groupExprs) {
        PhysicalPlan input = stack.peek();
        ArrowType[] arrowTypes = groupExprs.stream().map(i -> i.getType().getArrowType()).toArray(n -> new ArrowType[n]);
        Schema outputSchema = SchemaBuilder.ofArrowType(arrowTypes).toArrow();

        Map<Integer, ArrowType> indexTypeMap = new HashMap<>();
        for (int i = 0; i < outputSchema.getFields().size(); i++) {
            Field field = outputSchema.getFields().get(i);
            ArrowType type = field.getType();
            indexTypeMap.put(i, type);
        }
        castAggProject(indexTypeMap);

        input = stack.pop();
        outputSchema = input.schema();
        PhysicalPlan physicalPlan = null;


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

        stack.push(physicalPlan);
        return this;
    }
//
//    @NotNull
//    private AggregateVectorExpression[] getAggregateVectorExpressions(List<Accumulator2> groupExprs) {
//        int groupExprSize = groupExprs.size();
//        AggregateVectorExpression[] aggregateVectorExpressions = new AggregateVectorExpression[groupExprSize];
//        for (int i = 0; i < groupExprSize; i++) {
//            Accumulator2 groupByFunction = groupExprs.get(i);
//            AggregateVectorExpression vectorExpression = null;
//            switch (groupByFunction.getType()) {
//                case BooleanType:
//                case UInt8Type:
//                case UInt16Type:
//                case UInt32Type:
//                case UInt64Type:
//                case Int8Type:
//                case Int16Type:
//                case Int32Type:
//                case Int64Type:
//                    vectorExpression = LongExprAggregateVectorExpression.create((LongAccumulator) groupByFunction);
//                    break;
//                case FloatType:
//                case DoubleType:
//                    vectorExpression = DoubleExprAggregateVectorExpression.create((DoubleAccumulator) groupByFunction);
//                    break;
//                case StringType:
//                case BinaryType:
//                case TimeMilliType:
//                case DateType:
//                case DatetimeMilliType:
//                case SymbolType:
//                case ObjectType:
//                case NullType:
//                    break;
//            }
//            aggregateVectorExpressions[i] = vectorExpression;
//        }
//        return aggregateVectorExpressions;
//    }


    public PhysicalPlan build() {
        return stack.pop();
    }

}

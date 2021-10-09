//package core.aggregateCall;
//
//import core.AggregateVectorExpression;
//import core.InnerType;
//import core.baseexpression.groupby.DoubleAccumulator;
//import org.apache.arrow.vector.BaseIntVector;
//import org.apache.arrow.vector.FieldVector;
//import org.apache.arrow.vector.FloatingPointVector;
//import org.apache.arrow.vector.holders.Float8Holder;
//import org.apache.arrow.vector.holders.ValueHolder;
//
//public class DoubleExprAggregateVectorExpression implements AggregateVectorExpression {
//    final DoubleAccumulator groupByFunction;
//
//    public static DoubleExprAggregateVectorExpression create(DoubleAccumulator longAccumulator) {
//        return new DoubleExprAggregateVectorExpression(longAccumulator);
//    }
//
//    public DoubleExprAggregateVectorExpression(DoubleAccumulator groupByFunction) {
//        this.groupByFunction = groupByFunction;
//    }
//
//    @Override
//    public void initValue(ReduceContext resultValue) {
//        groupByFunction.computeFirst(resultValue);
//    }
//
//    @Override
//    public void computeFinalValue(ReduceContext reduceContext, ValueHolder valueHolder) {
//        groupByFunction.computeFinal(reduceContext, (Float8Holder) valueHolder);
//    }
//
//    @Override
//    public void computeUpdateValue(ReduceContext resultValue, FieldVector input) {
//        if (input instanceof FloatingPointVector) {
//            FloatingPointVector float8Vector = ((FloatingPointVector) input);
//            int valueCount = float8Vector.getValueCount();
//            if (float8Vector.getNullCount() > 0) {
//                for (int i = 0; i < valueCount; i++) {
//                    if (!float8Vector.isNull(i)) {
//                        groupByFunction.computeNext(resultValue, float8Vector.getValueAsDouble(i));
//                    }
//                }
//            } else {
//                for (int i = 0; i < valueCount; i++) {
//                    groupByFunction.computeNext(resultValue, float8Vector.getValueAsDouble(i));
//                }
//            }
//        } else if (input instanceof BaseIntVector) {
//            BaseIntVector baseIntVector = ((BaseIntVector) input);
//            int valueCount = baseIntVector.getValueCount();
//            if (baseIntVector.getNullCount() > 0) {
//                for (int i = 0; i < valueCount; i++) {
//                    if (!baseIntVector.isNull(i)) {
//                        groupByFunction.computeNext(resultValue, baseIntVector.getValueAsLong(i));
//                    }
//                }
//            } else {
//                for (int i = 0; i < valueCount; i++) {
//                    groupByFunction.computeNext(resultValue, baseIntVector.getValueAsLong(i));
//                }
//            }
//        }
//
//    }
//
//    @Override
//    public InnerType getType() {
//        return groupByFunction.getType();
//    }
//
//    @Override
//    public int getInputColumnIndex() {
//        return 0;
//    }
//
//    @Override
//    public ReduceContext createReduceContext() {
//        return groupByFunction.createReduceContext();
//    }
//}

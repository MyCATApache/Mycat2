//package core.aggregateCall;
//
//import core.AggregateVectorExpression;
//import core.InnerType;
//import core.baseexpression.groupby.LongAccumulator;
//import io.questdb.cairo.map.MapValue;
//import org.apache.arrow.vector.BaseIntVector;
//import org.apache.arrow.vector.FieldVector;
//import org.apache.arrow.vector.holders.ValueHolder;
//
//public class LongExprAggregateVectorExpression implements AggregateVectorExpression {
//    final LongAccumulator groupByFunction;
//
//    public static LongExprAggregateVectorExpression create(LongAccumulator longAccumulator) {
//        return new LongExprAggregateVectorExpression(longAccumulator);
//    }
//
//    public LongExprAggregateVectorExpression(LongAccumulator groupByFunction) {
//        this.groupByFunction = groupByFunction;
//    }
//
//    @Override
//    public void initValue(ReduceContext resultValue) {
//        groupByFunction.computeFirst(resultValue);
//    }
//
//    @Override
//    public void computeFinalValue(ReduceContext resultValue, ValueHolder output) {
//        groupByFunction.computeFinal(resultValue,output);
//    }
//
//    @Override
//    public void computeUpdateValue(ReduceContext resultValue, FieldVector input) {
//        int valueCount = input.getValueCount();
//        BaseIntVector bigIntVector = (BaseIntVector) input;
//        int nullCount = bigIntVector.getNullCount();
//        if (nullCount > 0) {
//            for (int rowId = 0; rowId < valueCount; rowId++) {
//                if (!bigIntVector.isNull(rowId)) {
//                    groupByFunction.computeNext(resultValue, bigIntVector.getValueAsLong(rowId));
//                }
//            }
//        } else {
//            for (int rowId = 0; rowId < valueCount; rowId++) {
//                groupByFunction.computeNext(resultValue, bigIntVector.getValueAsLong(rowId));
//            }
//        }
//    }
//
//    @Override
//    public void initValue(MapValue resultValue) {
//        groupByFunction.computeFirst(resultValue,);
//    }
//
//    @Override
//    public void computeFinalValue(MapValue resultValue, ValueHolder output) {
//
//    }
//
//    @Override
//    public void computeUpdateValue(MapValue resultValue, FieldVector input) {
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
//        return groupByFunction.getInputColumnIndex();
//    }
//
//    @Override
//    public void allocContext(InnerType[] columnTypes) {
//        groupByFunction.allocContext(columnTypes);
//    }
//}

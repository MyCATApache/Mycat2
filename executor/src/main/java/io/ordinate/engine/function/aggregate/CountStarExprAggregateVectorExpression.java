//package core.aggregateCall;
//
//import core.AggregateVectorExpression;
//import core.InnerType;
//import core.baseexpression.groupby.LongAccumulator;
//import org.apache.arrow.vector.BaseIntVector;
//import org.apache.arrow.vector.FieldVector;
//import org.apache.arrow.vector.holders.BigIntHolder;
//import org.apache.arrow.vector.holders.ValueHolder;
//
//public class CountStarExprAggregateVectorExpression implements AggregateVectorExpression {
//    final int inputColumnIndex;
//
//    public static CountStarExprAggregateVectorExpression create(int inputColumnIndex) {
//        return new CountStarExprAggregateVectorExpression(inputColumnIndex);
//    }
//
//    public CountStarExprAggregateVectorExpression(int inputColumnIndex) {
//        this.inputColumnIndex = inputColumnIndex;
//    }
//
//    @Override
//    public void initValue(ReduceContext resultValue) {
//        resultValue.putLong(0);
//    }
//
//    @Override
//    public void computeFinalValue(ReduceContext resultValue, ValueHolder output) {
//        BigIntHolder output1 = (BigIntHolder) output;
//        output1.value = resultValue.getLong();
//    }
//
//    @Override
//    public void computeUpdateValue(ReduceContext resultValue, FieldVector input) {
//        int valueCount = input.getValueCount();
//        resultValue.addLong(valueCount);
//    }
//
//    @Override
//    public InnerType getType() {
//        return InnerType.Int64Type;
//    }
//
//    @Override
//    public int getInputColumnIndex() {
//        return inputColumnIndex;
//    }
//
//    @Override
//    public LongReduceContext createReduceContext() {
//        return new LongReduceContext();
//    }
//}

//package core.aggregateCall;
//
//import core.Record;
//import core.baseexpression.Function;
//import core.baseexpression.LongFunction;
//import core.baseexpression.column.LongColumn;
//import core.baseexpression.groupby.Accumulator2;
//
//import java.util.Collections;
//import java.util.List;
//
//public class CountLongGroupByFunction extends LongFunction implements Accumulator2 {
//    private final int argIndex;
//    private int valueIndex;
//
//    public CountLongGroupByFunction(int argIndex) {
//        this.argIndex = argIndex;
//    }
//
//    @Override
//    public List<Function> getArgs() {
//        return Collections.singletonList(LongColumn.newInstance(argIndex));
//    }
//
//    @Override
//    public void computeFirst(ResultValue resultValue, Record record) {
//        long value = record.getLong(argIndex);
//        if (!record.isNull(argIndex)) {
//            resultValue.addLong(valueIndex, value);
//        }
//    }
//
//    @Override
//    public void computeNext(ResultValue resultValue, Record record) {
//        long value = record.getLong(argIndex);
//        if (!record.isNull(argIndex)) {
//            resultValue.addLong(valueIndex, value);
//        }
//    }
//
//    @Override
//    public void init(int columnIndex) {
//        this.valueIndex = columnIndex;
//    }
//
//    @Override
//    public int getOutputColumnIndex() {
//        return valueIndex;
//    }
//
//    @Override
//    public long getLong(Record rec) {
//        return rec.getLong(this.valueIndex);
//    }
//}

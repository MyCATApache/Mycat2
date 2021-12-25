//package physicalplan;
//
//import com.carrotsearch.hppc.*;
//import com.google.common.collect.ImmutableList;
//
//import io.reactivex.rxjava3.core.Observable;
//import io.reactivex.rxjava3.schedulers.Schedulers;
//import io.ordinate.engine.vectorexpression.AggregateVectorExpression;
//import io.mycat.beans.mycat.ArrowTypes;
//import io.ordinate.engine.record.RootContext;
//import org.apache.arrow.memory.util.hash.SimpleHasher;
//import org.apache.arrow.vector.*;
//import org.apache.arrow.vector.compare.Range;
//import org.apache.arrow.vector.compare.RangeEqualsVisitor;
//import org.apache.arrow.vector.complex.MapVector;
//import org.apache.arrow.vector.types.pojo.ArrowType;
//import org.apache.arrow.vector.types.pojo.Schema;
//import org.apache.hive.com.esotericsoftware.kryo.util.IntArray;
//
//import java.util.List;
//
//public class HashAgg implements PhysicalPlan {
//    boolean keysSorted = false;
//    final PhysicalPlan input;
//    final int[] groupExprs;
//    final AggregateVectorExpression[] aggregateExprs;
//    private Schema schema;
//
//    public static HashAgg create(PhysicalPlan input, final int[] groupExpr, final AggregateVectorExpression[] aggregateExprs, Schema schema) {
//        return new HashAgg(input, groupExpr, aggregateExprs, schema);
//    }
//
//    public HashAgg(PhysicalPlan input, final int[] groupExpr, final AggregateVectorExpression[] aggregateExprs, Schema schema) {
//        this.input = input;
//        this.groupExprs = groupExpr;
//        this.aggregateExprs = aggregateExprs;
//        this.schema = schema;
//    }
//
//    @Override
//    public Schema schema() {
//        return this.schema;
//    }
//
//    @Override
//    public List<PhysicalPlan> children() {
//        return ImmutableList.of(input);
//    }
//
//
//    @Override
//    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
//        return input.execute(rootContext).subscribeOn(Schedulers.single())
//                .reduce(createAggContext(rootContext),
//                        (aggContext, vectorSchemaRoot2) -> aggContext.reduce(vectorSchemaRoot2))
//                .map(i -> i.finalToVectorSchemaRoot()).toObservable();
//    }
//
//    @Override
//    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
//        physicalPlanVisitor.visit(this);
//    }
//
//    public static interface AggContext {
//
//        public AggContext reduce(VectorSchemaRoot root);
//
//        public VectorSchemaRoot finalToVectorSchemaRoot();
//    }
//
//    public AggContext createAggContext(RootContext rootContext) {
//        VectorSchemaRoot output = rootContext.getVectorSchemaRoot(schema());
//        FieldVector[] keysVectors = new FieldVector[groupExprs.length];
//        for (int index = 0; index < groupExprs.length; index++) {
//            keysVectors[index] = output.getVector(index);
//        }
//        IntObjectHashMap<IntArray> map = new IntObjectHashMap<>();
//        return new AggContext() {
//            int ids = 0;
//
//            @Override
//            public AggContext reduce(VectorSchemaRoot input) {
//                int rowCount = input.getRowCount();
//                List<FieldVector> inputFieldVectors = input.getFieldVectors();
//                int[] hashVector = new int[rowCount];
//                int[] recordIdVector = new int[rowCount];
//                int recordRowIds = 0;
//                for (int groupIndex : groupExprs) {
//                    FieldVector valueVectors = inputFieldVectors.get(groupIndex);
//                    for (int rowId = 0; rowId < rowCount; rowId++) {
//                        hashVector[rowId] += valueVectors.hashCode(rowId, SimpleHasher.INSTANCE) * (17 * 37);
//                    }
//                }
//                for (int rowId = 0; rowId < rowCount; rowId++) {
//                    int hash = hashVector[rowId];
//                    int recordId = -1;
//                    boolean find = false;
//                    if (map.containsKey(hash)) {
//                        IntArray intList = map.get(hash);
//                        for (int eachRecordId : intList.items) {
//                            boolean allMatch = true;
//                            for (int index = 0; allMatch && index < groupExprs.length; index++) {
//                                int groupIndex = groupExprs[index];
//                                allMatch = equalsVector(keysVectors[groupIndex], inputFieldVectors.get(groupIndex), eachRecordId, rowId);
//                            }
//                            if (allMatch) {
//                                recordId = eachRecordId;
//                                find = true;
//                                break;
//                            }
//                        }
//                    }
//                    if (!find) {
//                        recordId = ids++;
//                        for (int index = 0; index < groupExprs.length; index++) {
//                            int groupIndex = groupExprs[index];
//                            keysVectors[index].copyFrom(rowId, recordId, inputFieldVectors.get(groupIndex));
//                        }
//                        IntArray intList = new IntArray();
//                        intList.add(recordId);
//                        map.put(hash, intList);
//                    }
//                    recordIdVector[recordRowIds++] = recordId;
//                }
//
//                for (AggregateVectorExpression aggregateExpr : aggregateExprs) {
//                    //aggregateExpr.computeUpdateValue(recordIdVector, keysVectors, input, output);
//                }
//                return this;
//            }
//
//            @Override
//            public VectorSchemaRoot finalToVectorSchemaRoot() {
//                for (int i = 0; i < aggregateExprs.length; i++) {
//                  //  aggregateExprs[i].computeFinalValue(output.getVector(i));
//                }
//                close();
//                return output;
//            }
//
//            void close() {
//            }
//        };
//
//    }
//
//
//    private static boolean equalsVector(FieldVector left, FieldVector right, int leftIndex, int rightIndex) {
//        RangeEqualsVisitor rangeEqualsVisitor = new RangeEqualsVisitor(left, right);
//        Range range = new Range(leftIndex, rightIndex, 1);
//        return rangeEqualsVisitor.rangeEquals(range);
//    }
//
//
//    private void create_hashes(List<FieldVector> keyList, IntVector resVector, MapVector keyHolder) {
//        IntHashSet set = new IntHashSet();
//        boolean multiCol = keyList.size() > 1;
//        for (FieldVector vectors : keyList) {
//            ArrowType type = vectors.getField().getType();
//            if (type == ArrowTypes.Int32Type) {
//                IntVector int32Value = (IntVector) vectors;
//                int valueCount = int32Value.getValueCount();
//                int nullCount = int32Value.getNullCount();
//                if (nullCount == 0) {
//                    if (!multiCol) {
//                        for (int i = 0; i < valueCount; i++) {
//                            resVector.set(i, Integer.hashCode(int32Value.get(i)));
//                        }
//                    } else {
//                        for (int i = 0; i < valueCount; i++) {
//                            resVector.set(i, combine_hashes(resVector.get(i), Integer.hashCode(int32Value.get(i))));
//                        }
//                    }
//                } else {
//                    if (!multiCol) {
//                        for (int i = 0; i < valueCount; i++) {
//                            if (!int32Value.isNull(i)) {
//                                resVector.set(i, Integer.hashCode(int32Value.get(i)));
//                            } else {
//                                resVector.set(i, 0);
//                            }
//                        }
//                    }
//                }
//
//            }
//        }
//
//    }
//
//    public static int combine_hashes(int l, int r) {
//        return (17 * 37 + l) * (37) + r;
//    }
//}

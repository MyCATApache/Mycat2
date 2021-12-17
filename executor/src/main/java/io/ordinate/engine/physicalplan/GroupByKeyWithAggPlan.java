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

package io.ordinate.engine.physicalplan;

import io.ordinate.engine.record.*;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.builder.GroupKeys;
import io.ordinate.engine.function.aggregate.AccumulatorFunction;
import io.ordinate.engine.schema.IntInnerType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.ordinate.engine.structure.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.functions.BiFunction;
import io.reactivex.rxjava3.functions.Function;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class GroupByKeyWithAggPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(GroupByKeyWithAggPlan.class);
    final PhysicalPlan physicalPlan;
    final GroupKeys[] groupByKeys;
    private AccumulatorFunction[] accumulators;
    private io.ordinate.engine.function.Function[] functions;
    final Schema schema;
    FunctionSink functionSink;
    RecordSink outputSink;

    public GroupByKeyWithAggPlan(PhysicalPlan input,GroupKeys[] groupByKeys, AccumulatorFunction[] accumulators, Schema schema) {
        this.physicalPlan = input;
        this.groupByKeys = groupByKeys;
        this.accumulators = accumulators;
        this.schema = schema;
        this.functions = new io.ordinate.engine.function.Function[accumulators.length];
        for(int i = 0; i < accumulators.length; ++i) {
            this.functions[i] = accumulators[i];
        }

        this.functionSink = RecordSinkFactory.INSTANCE.buildFunctionSink(this.functions);
        outputSink = RecordSinkFactory.INSTANCE.buildRecordSink(getIntTypes());
    }

    @Override
    public Schema schema() {
        return this.schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.singletonList(physicalPlan);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        List<Field> fields = schema().getFields();
        InnerType[] innerTypes = schema().getFields().stream().map(i -> InnerType.from(i.getType())).toArray(n -> new InnerType[n]);


        if (groupByKeys.length > 0) {
            ColumnTypes arrayColumnTypes = RecordUtil.getArrayColumnTypes(accumulators);
            Map map = MapFactory.createMap2(innerTypes, arrayColumnTypes);
            RecordSink[] recordSinks = buildRecordSink(fields);


            return physicalPlan.execute(rootContext).reduce(map, (mapKey, input) -> {
                int rowCount = input.getRowCount();
                VectorBatchRecord record = new VectorBatchRecord(input);
                for (RecordSink recordSink : recordSinks) {
                    for (int rowId = 0; rowId < rowCount; rowId++) {
                        record.setPosition(rowId);
                        MapKey key = mapKey.withKey();
                        RecordSetter recordSinkSPI = RecordSinkFactory.INSTANCE.getRecordSinkSPI(key);
                        recordSink.copy(record, recordSinkSPI);
                        MapValue value = key.createValue();
                        if (value.isNew()) {
                            for (AccumulatorFunction accumulator : accumulators) {
                                accumulator.computeFirst(value, record);
                            }
                        } else {
                            for (AccumulatorFunction accumulator : accumulators) {
                                accumulator.computeNext(value, record);
                            }
                        }
                    }
                }
                physicalPlan.eachFree(input);
                return mapKey;
            }).map(map1 -> {
                int size = (int) map1.size();
                VectorSchemaRoot output = rootContext.getVectorSchemaRoot(schema(), size);
                RecordCursor cursor = map1.getCursor();
                cursor.toTop();
                int index = 0;
                while (cursor.hasNext()) {
                    Record record = cursor.getRecord();
                    functionSink.copy(accumulators, RecordUtil.wrapAsAggRecord(record), index++, output);
                }
                output.setRowCount(index);
                return output;
            }).toObservable().doOnComplete(() -> map.close());
        } else {
            SimpleMapValue mapValue = new SimpleMapValue(RecordUtil.getContextSize(accumulators));
            return physicalPlan.execute(rootContext).reduce(mapValue, new BiFunction<SimpleMapValue, VectorSchemaRoot, SimpleMapValue>() {
                AtomicBoolean first = new AtomicBoolean(true);

                @Override
                public SimpleMapValue apply(SimpleMapValue simpleMapValue, VectorSchemaRoot root) throws Throwable {
                    int rowCount = root.getRowCount();
                    VectorBatchRecord record = new VectorBatchRecord(root);
                    if (first.compareAndSet(true, false)) {
                        record.setPosition(0);
                        for (AccumulatorFunction accumulator : accumulators) {
                            accumulator.computeFirst(mapValue, record);
                        }
                        for (int i = 1; i < rowCount; i++) {
                            record.setPosition(i);
                            for (AccumulatorFunction accumulator : accumulators) {
                                accumulator.computeNext(mapValue, record);
                            }
                        }
                    } else {
                        for (int i = 0; i < rowCount; i++) {
                            record.setPosition(i);
                            for (AccumulatorFunction accumulator : accumulators) {
                                accumulator.computeNext(mapValue, record);
                            }
                        }

                    }
                    root.close();
                    return simpleMapValue;
                }
            }).map(new Function<SimpleMapValue, VectorSchemaRoot>() {
                @Override
                public VectorSchemaRoot apply(SimpleMapValue simpleMapValue) throws Throwable {
                    VectorSchemaRoot vectorSchemaRoot = rootContext.getVectorSchemaRoot(schema(), 1);
                    vectorSchemaRoot.setRowCount(1);
                    functionSink.copy(accumulators, RecordUtil.wrapAsAggRecord(simpleMapValue), 0, vectorSchemaRoot);
                    return vectorSchemaRoot;
                }
            }).toObservable();
        }
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }

    @NotNull
    private RecordSink[] buildRecordSink(List<Field> fields) {
        RecordSink[] recordSinks = new RecordSink[groupByKeys.length];
        int groupIndex = 0;
        for (GroupKeys groupByKey : groupByKeys) {
            IntInnerType[] intPairs = new IntInnerType[groupByKey.getKeys().length];
            int[] keys = groupByKey.getKeys();
            int index = 0;
            for (int key : keys) {
                Field field = fields.get(key);
                intPairs[index] = IntInnerType.of(index, InnerType.from(field.getType()));
                index++;
            }
            recordSinks[groupIndex] = RecordSinkFactory.INSTANCE.buildRecordSink(intPairs);

            groupIndex++;

        }
        return recordSinks;
    }
}

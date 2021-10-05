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

import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.record.VectorBatchRecord;
import io.ordinate.engine.builder.GroupKeys;
import io.ordinate.engine.record.RecordSink;
import io.ordinate.engine.record.RecordSinkFactory;
import io.ordinate.engine.record.RecordSetter;
import io.ordinate.engine.schema.IntInnerType;
import io.questdb.cairo.map.Map;
import io.ordinate.engine.structure.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableSource;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.Function;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class GroupByKeyPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(GroupByKeyPlan.class);
    final PhysicalPlan inputPlan;
    final GroupKeys[] groupByKeys;
    final Schema schema;

    public GroupByKeyPlan(PhysicalPlan input, GroupKeys[] groupByKeys, Schema schema) {
        this.inputPlan = input;
        this.groupByKeys = groupByKeys;
        this.schema = schema;
    }

    @Override
    public Schema schema() {
        return this.schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.singletonList(inputPlan);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        List<Field> fields = schema().getFields();
        InnerType[] innerTypes = schema().getFields().stream().map(i -> InnerType.from(i.getType())).toArray(n -> new InnerType[n]);
        Map map = MapFactory.createMap(innerTypes);

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
        return inputPlan.execute(rootContext).flatMap(new Function<VectorSchemaRoot, ObservableSource<? extends VectorSchemaRoot>>() {
            @Override
            public ObservableSource<? extends VectorSchemaRoot> apply(VectorSchemaRoot input) throws Throwable {
                int rowCount = input.getRowCount();
                VectorBatchRecord record = new VectorBatchRecord(input);
                VectorSchemaRoot output = rootContext.getVectorSchemaRoot(schema, rowCount*recordSinks.length);
                int outputRowId= 0;
                for (int i = 0; i < recordSinks.length; i++) {
                    RecordSink recordSink = recordSinks[i];
                    for (int rowId = 0; rowId < rowCount; rowId++) {
                        record.setPosition(rowId);
                        MapKey key = map.withKey();
                        RecordSetter recordSinkSPI = RecordSinkFactory.INSTANCE.getRecordSinkSPI(key);
                        recordSink.copy(record, recordSinkSPI);

                        if (key.create()) {
                            recordSink.copy(record, outputRowId, output);
                            outputRowId++;
                            //output
                        } else {
                            //skip
                        }
                    }

                }
                if (outputRowId == 0){
                    output.close();
                    return Observable.empty();
                }
                output.setRowCount(outputRowId);
                inputPlan.eachFree(input);
                return Observable.fromArray(output);
            }
        }).doOnComplete(new Action() {
            @Override
            public void run() throws Throwable {
                map.close();
            }
        });
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }
}

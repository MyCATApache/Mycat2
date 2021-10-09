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

import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.record.VectorBatchRecord;
import io.ordinate.engine.record.RecordSink;
import io.ordinate.engine.record.RecordSinkFactory;
import io.ordinate.engine.record.RecordSetter;
import io.ordinate.engine.schema.IntInnerType;
import io.questdb.cairo.map.Map;
import io.ordinate.engine.structure.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.functions.Action;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class DistinctPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistinctPlan.class);
    final PhysicalPlan input;
    final int[] columnIndexes;

    public DistinctPlan(PhysicalPlan input, int[] columnIndexes) {
        this.input = input;
        this.columnIndexes = columnIndexes;
    }

    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.singletonList(input);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        //DistinctContext distinctContext = new DistinctContext();
      IntInnerType[] intPairs = getIntTypes();

        Map map = MapFactory.createMap(intPairs);
        RecordSink recordSink = RecordSinkFactory.INSTANCE.buildRecordSink(intPairs);
        return input.execute(rootContext).flatMap(input -> {
            int rowCount = input.getRowCount();
            int columnCount = input.getFieldVectors().size();
            VectorSchemaRoot output = rootContext.getVectorSchemaRoot(schema(), rowCount);
            VectorBatchRecord record = new VectorBatchRecord(input);

            int outputRowId = 0;
            for (int rowId = 0; rowId < rowCount; rowId++) {
                record.setPosition(rowId);
                MapKey key = map.withKey();

                RecordSetter recordSinkSPI = RecordSinkFactory.INSTANCE.getRecordSinkSPI(key);

                recordSink.copy(record, recordSinkSPI);
                if (key.create()) {
                    recordSink.copy(record,outputRowId,output);
                    outputRowId++;
                    //output
                } else {
                    //skip
                }
            }
            if (outputRowId == 0){
                output.close();
                return Observable.empty();
            }
            output.setRowCount(outputRowId);

            return Observable.fromArray(output);
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


    static class DistinctContext {
        long rowId;
        VectorSchemaRoot output;
    }
}

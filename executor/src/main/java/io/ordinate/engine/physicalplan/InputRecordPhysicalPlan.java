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

import io.ordinate.engine.record.Record;
import io.ordinate.engine.record.RecordSink;
import io.ordinate.engine.record.RecordSinkFactory;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class InputRecordPhysicalPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(InputRecordPhysicalPlan.class);
    final Schema schema;
    final Observable<Record> observable;
    final RecordSink recordSink;

    public static InputRecordPhysicalPlan create(Schema schema, Observable<Record> observable) {
        return new InputRecordPhysicalPlan(schema, observable);
    }

    public InputRecordPhysicalPlan(Schema schema, Observable<Record> observable) {
        this.schema = schema;
        this.observable = observable;
        this.recordSink = RecordSinkFactory.INSTANCE.buildRecordSink(getIntTypes());
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.emptyList();
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        return Observable.create(emitter -> {
            VectorSchemaRoot vectorSchemaRoot = null;
            final int batchSize = rootContext.getBatchSize();
            int batchId = 0;
            for (Record record : observable.blockingIterable()) {

                if (batchId >= batchSize) {
                    emitter.onNext(vectorSchemaRoot);
                    vectorSchemaRoot = null;
                }
                if (vectorSchemaRoot == null) {
                    vectorSchemaRoot = rootContext.getVectorSchemaRoot(schema(), batchSize);
                    batchId = 0;
                }
                recordSink.copy(record, batchId, vectorSchemaRoot);
                ++batchId;
            }
        });
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        throw new UnsupportedOperationException();
    }
}

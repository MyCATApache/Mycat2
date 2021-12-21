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

import com.google.common.collect.Iterables;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.builder.SchemaBuilder;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class ValuesPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValuesPlan.class);
    final Schema schema;
    Iterable<Object[]>  rowList;
    public  static ValuesPlan create(Schema schema, Iterable<Object[]> rowList) {
        return new ValuesPlan(schema,rowList);
    }



    public ValuesPlan(Schema schema, Iterable<Object[]> rowList) {
        this.schema = schema;
        this.rowList = rowList;
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
    public void eachFree(VectorSchemaRoot vectorSchemaRoot) {
        vectorSchemaRoot.close();
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        return Observable.create(emitter -> {
            try {
                int columnSize = schema.getFields().size();
                for (List<Object[]> objects : Iterables.partition(rowList, rootContext.getBatchSize())) {
                    int rowCount = objects.size();
                    VectorSchemaRoot vectorSchemaRoot = rootContext.getVectorSchemaRoot(schema,rowCount);
                    for (int columnId = 0; columnId < columnSize; columnId++) {
                        FieldVector vector = vectorSchemaRoot.getVector(columnId);
                        for (int rowId = 0; rowId <rowCount; rowId++) {
                            Object o = objects.get(rowId)[columnId];
                            SchemaBuilder.setVector(vector,rowId,o);
                        }
                    }
                    vectorSchemaRoot.setRowCount(rowCount);
                    emitter.onNext(vectorSchemaRoot);
                }
                emitter.onComplete();
            }catch (Exception e){
                emitter.tryOnError(e);
            }
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }
}

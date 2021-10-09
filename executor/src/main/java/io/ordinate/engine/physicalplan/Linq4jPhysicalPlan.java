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

import com.google.common.collect.ImmutableList;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.builder.SchemaBuilder;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.reactivex.rxjava3.core.ObservableSource;
import io.reactivex.rxjava3.functions.Function;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class Linq4jPhysicalPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(Linq4jPhysicalPlan.class);
    final PhysicalPlan input;

    public Linq4jPhysicalPlan(PhysicalPlan input) {
        this.input = input;
    }

    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public List<PhysicalPlan> children() {
        return ImmutableList.of(input);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        int columnCount = schema().getFields().size();
        return input.execute(rootContext).flatMap((Function<VectorSchemaRoot, ObservableSource<Object[]>>) root -> Observable.create((ObservableOnSubscribe<Object[]>) emitter -> {
            int rowCount = root.getRowCount();
            List<FieldVector> fieldVectors = root.getFieldVectors();
            for (int i = 0; i < rowCount; i++) {
                Object[] row = new Object[columnCount];
                for (int columnId = 0; columnId < columnCount; columnId++) {
                    FieldVector valueVectors = fieldVectors.get(columnId);
                    row[columnId] = valueVectors.getObject(columnId);
                    emitter.onNext(map(row));
                }
            }
            root.close();
            emitter.onComplete();
        })).buffer(rootContext.getBatchSize()).map(objects -> {
            VectorSchemaRoot vectorSchemaRoot = rootContext.getVectorSchemaRoot(schema());
            for (int columnId = 0; columnId < columnCount; columnId++) {
                FieldVector vector = vectorSchemaRoot.getVector(columnId);
                for (int i = 0; i < objects.size(); i++) {
                    Object o = objects.get(i)[columnId];
                    if (o != null) {
                        SchemaBuilder.setVector(vector, i, o);
                    } else {
                        SchemaBuilder.setVectorNull(vector, i);
                    }
                }
            }
            return vectorSchemaRoot;
        });
    }

    public abstract Object[] map(Object[] objects);
}

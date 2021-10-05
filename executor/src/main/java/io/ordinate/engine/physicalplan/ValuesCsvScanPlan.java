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
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ValuesCsvScanPlan extends CsvScanPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValuesCsvScanPlan.class);
    private final List<VectorSchemaRoot> observable;

    public ValuesCsvScanPlan(String path, Schema schema, RootContext rootContext) {
        super(path, schema);
        observable = (List)super.execute(rootContext).toList().blockingGet();

        int size= 0;
        for (VectorSchemaRoot vectorSchemaRoot : observable) {
            size+=vectorSchemaRoot.getRowCount();
        }
        LOGGER.debug(" all size:{}",size);

    }

    @Override
    public Observable execute(RootContext rootContext) {
        return (Observable)Observable.create((ObservableOnSubscribe<VectorSchemaRoot>) emitter -> {
            int size = 0 ;
            for (VectorSchemaRoot vectorSchemaRoot : observable) {
                size+=vectorSchemaRoot.getRowCount();
                emitter.onNext(vectorSchemaRoot);
            }
            LOGGER.debug(" size:{}",size);
            emitter.onComplete();
        });
    }

    @Override
    public String toString() {
        return "ValuesCsvScan:" + path;
    }

    @Override
    public void eachFree(VectorSchemaRoot vectorSchemaRoot) {

    }

    @Override
    public void close() {
        for (VectorSchemaRoot vectorSchemaRoot : observable) {
            vectorSchemaRoot.close();
        }
    }
}

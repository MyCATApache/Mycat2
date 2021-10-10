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
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.SneakyThrows;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

public class CsvScanPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvScanPlan.class);
    protected final String path;
    protected final org.apache.arrow.vector.types.pojo.Schema arrowSchema;

    public CsvScanPlan(String path, Schema schema) {
        this.path = path;
        this.arrowSchema = schema;
    }


    @Override
    public org.apache.arrow.vector.types.pojo.Schema schema() {
        return this.arrowSchema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return ImmutableList.of();
    }

    @Override
    @SneakyThrows
    public Observable execute(RootContext rootContext) {
        return (Observable) Observable.create(emitter -> {

            int columnNum = arrowSchema.getFields().size();
            Path path = Paths.get(this.path);
            long size = Files.size(path);
//          int estimateRowCount = (int) Math.max(size / columnNum / 22, pContext.getBatchSize());
            int estimateRowCount =900_0000;
            LOGGER.debug("estimateRowCount:{}",estimateRowCount);
            try (CSVParser parser = CSVParser.parse(path, StandardCharsets.UTF_8, CSVFormat.RFC4180)) {

                Iterator<CSVRecord> iterator = parser.iterator();

                int batchSize = estimateRowCount;
                VectorSchemaRoot vectorSchemaRoot = null;

                int batchCount = 0;
                while (iterator.hasNext()) {
                    CSVRecord record = iterator.next();
                    if (vectorSchemaRoot==null) {
                        vectorSchemaRoot = rootContext.getVectorSchemaRoot(arrowSchema, estimateRowCount);
                        vectorSchemaRoot.setRowCount(estimateRowCount);
                    }
                    List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
                    for (int i = 0; i < columnNum; i++) {
                        String s = record.get(i);
                        FieldVector valueVectors = fieldVectors.get(i);
                        if (s == null) {
                            SchemaBuilder.setVectorNull(valueVectors, batchCount);
                        } else {
                            SchemaBuilder.setVector(valueVectors, batchCount, s);
                        }
                    }
                    batchCount++;
                    if (batchCount == batchSize) {
                        vectorSchemaRoot.setRowCount(batchCount);
                        emitter.onNext(vectorSchemaRoot);
                        vectorSchemaRoot = null;
                        batchCount = 0;
                    }
                }
                if (vectorSchemaRoot != null) {
                    vectorSchemaRoot.setRowCount(batchCount);
                    emitter.onNext(vectorSchemaRoot);
                    batchCount = 0;
                    vectorSchemaRoot = null;
                }
                long recordNumber = parser.getRecordNumber();
                System.out.println("recordNumber:" + recordNumber);

                emitter.onComplete();
            } catch (Exception e) {
                emitter.onError(e);
            }
        }).subscribeOn(Schedulers.io());

    }

    @Override
    public void eachFree(VectorSchemaRoot vectorSchemaRoot) {
        vectorSchemaRoot.close();
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }

    @Override
    public String toString() {
        return "CsvScan: schema=" + arrowSchema;
    }
}

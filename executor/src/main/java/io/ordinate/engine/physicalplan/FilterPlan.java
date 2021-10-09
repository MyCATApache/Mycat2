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

import io.ordinate.engine.function.Function;
import io.ordinate.engine.record.VectorBatchRecord;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.vector.ContinueFilterContext;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FilterPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterPlan.class);
    final PhysicalPlan inputPlan;
    final Function condition;
    final org.apache.arrow.vector.types.pojo.Schema schema;

    public FilterPlan(PhysicalPlan input, Function condition, org.apache.arrow.vector.types.pojo.Schema schema) {
        this.inputPlan = input;
        this.condition = condition;
        this.schema = schema;
    }

    @Override
    public org.apache.arrow.vector.types.pojo.Schema schema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return ImmutableList.of(inputPlan);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        return inputPlan.execute(rootContext).map(input -> {
            int rowCount = input.getRowCount();
            BitVector bitVector = new BitVector("", rootContext.getRootAllocator());
            bitVector.allocateNew(rowCount);
            VectorBatchRecord record = new VectorBatchRecord(input);
            try {
                for (int i = 0; i <rowCount; i++) {
                    record.setPosition(i);
                    int value = condition.getInt(record);
                    if (value>0){
                        bitVector.setToOne(i);
                    }else {
                        bitVector.set(i,0);
                    }
                }
                ContinueFilterContext continueContext = new ContinueFilterContext();
                continueContext.bitVector = bitVector;
                continueContext.input = input;
                return continueContext;
            } catch (Exception e) {
                input.close();
                throw e;
            }
        }).subscribeOn(Schedulers.computation()).map(inputContext -> {
            VectorSchemaRoot input = inputContext.input;
            try (BitVector booleanVector = inputContext.bitVector) {
                int rowCount = input.getRowCount();
                VectorSchemaRoot output = rootContext.getVectorSchemaRoot(schema, rowCount);
                int columnCount = input.getFieldVectors().size();
                int outputRowIndex = 0;
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    if (booleanVector.get(rowIndex) == 1) {
                        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                            FieldVector valueVectors = input.getFieldVectors().get(columnIndex);
                            output.getFieldVectors().get(columnIndex).copyFrom(rowIndex, outputRowIndex, valueVectors);
                        }
                        outputRowIndex++;
                    }
                }
                output.setRowCount(outputRowIndex);
                return output;
            }finally {
                inputPlan.eachFree(input);
            }
        }).subscribeOn(Schedulers.computation());
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }

    @Override
    public String toString() {
        return "Filter:" + condition;
    }
}

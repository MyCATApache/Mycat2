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
import io.ordinate.engine.builder.SortOptions;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.builder.PhysicalSortProperty;
import io.reactivex.rxjava3.core.Observable;
import lombok.Getter;
import org.apache.arrow.algorithm.sort.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class SortPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(SortPlan.class);
    final PhysicalPlan input;
    private List<PhysicalSortProperty> physicalSortProperties;

    public static SortPlan create(PhysicalPlan input, List<PhysicalSortProperty> physicalSortProperties){
        return new SortPlan(input, physicalSortProperties);
    }

    public SortPlan(PhysicalPlan input, List<PhysicalSortProperty> physicalSortProperties) {
        this.input = input;
        this.physicalSortProperties = physicalSortProperties;
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
        return input.execute(rootContext).reduce((root, root2) -> {
            NLJoinPlan.merge(root,root2);
            root2.close();
            return root;
        }).map(input -> {
            int rowCount = input.getRowCount();
            int columnCount = schema().getFields().size();
            BufferAllocator rootAllocator = rootContext.getRootAllocator();
            try {
                IntVector indexes = lexQuickSort(rootAllocator,input, physicalSortProperties.stream().map(i -> i.evaluateToSortColumn(input)).collect(Collectors.toList()));
                VectorSchemaRoot output = rootContext.getVectorSchemaRoot(schema(), rowCount);
                for (int targetIndex = 0; targetIndex < rowCount; targetIndex++) {
                    int sourceIndex = indexes.get(targetIndex);
                    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                        output.getFieldVectors().get(columnIndex)
                                .copyFrom(sourceIndex, targetIndex, input.getFieldVectors().get(columnIndex));
                    }
                }
                output.setRowCount(rowCount);
                return output;
            } finally {
                input.close();
            }
        }).toObservable();
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }

    @Getter
    public static class SortColumn {
        final FieldVector values;
        final SortOptions options;

        public SortColumn(FieldVector values, SortOptions options) {
            this.values = values;
            this.options = options;
        }
    }

    public IntVector lexQuickSort(BufferAllocator allocator, VectorSchemaRoot input, List<SortColumn> physicalSortExprs) {
        VectorValueComparator[] comparators = physicalSortExprs.stream().map(p -> buildCompare(p)).toArray(size -> new VectorValueComparator[size]);
        CompositeVectorComparator compositeVectorComparator = new CompositeVectorComparator(comparators);
        int valueCount = physicalSortExprs.get(0).values.getValueCount();
        IntVector valueVectors = new IntVector("", allocator);
        valueVectors.setInitialCapacity(valueCount);
        valueVectors.allocateNew(valueCount);
        valueVectors.setValueCount(valueCount);
        for (int index = 0; index < valueCount; index++) {
            valueVectors.set(index, index);
        }

        FixedWidthInPlaceVectorSorter fixedWidthInPlaceVectorSorter = new FixedWidthInPlaceVectorSorter();
        fixedWidthInPlaceVectorSorter.sortInPlace(valueVectors, compositeVectorComparator);
        return valueVectors;
    }

    private VectorValueComparator buildCompare(SortColumn p) {
        FieldVector values = p.getValues();
        SortOptions options = p.getOptions();
        VectorValueComparator<FieldVector> defaultComparator = DefaultVectorComparators.createDefaultComparator(values);
        defaultComparator.attachVectors(values,values);
        return new VectorValueComparator() {
            @Override
            public int getValueWidth() {
                return super.getValueWidth();
            }

            @Override
            public void attachVector(ValueVector vector) {
                super.attachVector(vector);
            }

            @Override
            public void attachVectors(ValueVector vector1, ValueVector vector2) {
                super.attachVectors(vector1, vector2);
            }

            @Override
            public int compare(int index1, int index2) {
                boolean isNull1 = values.isNull(index1);
                boolean isNull2 = values.isNull(index2);

                if (isNull1 || isNull2) {
                    if (isNull1 && isNull2) {
                        return 0;
                    } else if (isNull1) {
                        if(options.nullsFirst){
                            return -1;       // null1 is smaller
                        }else {
                            return 1;
                        }
                    } else {
                        if(options.nullsFirst){
                            return 1;       // null2 is smaller
                        }else {
                            return -1;
                        }
                    }
                }
                return compareNotNull(index1, index2);
            }

            @Override
            public int compareNotNull(int index1, int index2) {

               return defaultComparator.compareNotNull(index1,index2);
            }

            @Override
            public VectorValueComparator createNew() {
                return this;
            }
        };
    }


}

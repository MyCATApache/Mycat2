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
import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.function.Function;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.JoinType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;

public class NLJoinPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(NLJoinPlan.class);
    PhysicalPlan left;
    PhysicalPlan right;
    Function predicate;
    Schema schema;
    JoinType joinType;

    public static NLJoinPlan create(PhysicalPlan left, PhysicalPlan right, JoinType joinType, Function predicate, Schema schema, int id) {
        NLJoinPlan nlJoin = new NLJoinPlan(left, right, joinType, predicate, schema);
        return nlJoin;
    }

    public NLJoinPlan(PhysicalPlan left, PhysicalPlan right, JoinType joinType, Function predicate, Schema schema) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.predicate = predicate;
        this.schema = schema;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return ImmutableList.of(left, right);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        return left.execute(rootContext).reduce(rootContext.getVectorSchemaRoot(left.schema()), (leftInput, newBatch) -> {
            merge(leftInput, newBatch);
            newBatch.close();
            return leftInput;
        }).map(leftInput -> {
            Iterable<VectorSchemaRoot> rightBatchIterable = null;
            try {
                rightBatchIterable = right.execute(rootContext).toList().blockingGet();
                int rowCount = leftInput.getRowCount();
                VectorSchemaRoot output = rootContext.getVectorSchemaRoot(schema(), rowCount);
                int rightColumnSize = right.schema().getFields().size();
                int leftColumnSize = left.schema().getFields().size();
                int rowId = 0;

                for (int leftIndex = 0; leftIndex < rowCount; leftIndex++) {
                    for (VectorSchemaRoot rightBatch : rightBatchIterable) {
                        for (int rightRowId = 0; rightRowId < rightBatch.getRowCount(); rightRowId++) {
                            Record record = createRecord(leftInput, leftIndex, rightBatch, rightRowId);
                            boolean match = predicate.getInt(record) > 0;
                            switch (joinType) {
                                case INNER:
                                    if (match) {

                                        assign(leftInput, output, rightColumnSize, leftColumnSize, rowId, rightRowId, rightBatch);
                                        rowId++;
                                    }
                                    break;
                                case LEFT:
                                    if (!match) {
                                        assignLeft(leftInput, output, leftColumnSize, rowId);
                                    } else {
                                        assign(leftInput, output, rightColumnSize, leftColumnSize, rowId, rightRowId, rightBatch);
                                    }
                                    rowId++;
                                    break;
                                case RIGHT:
                                    if (!match) {
                                        assignRight(output, rightColumnSize, leftColumnSize, rowId, rightRowId, rightBatch);
                                    } else {
                                        assign(leftInput, output, rightColumnSize, leftColumnSize, rowId, rightRowId, rightBatch);
                                    }  rowId++;

                                    break;
                                case FULL:

                                    assign(leftInput, output, rightColumnSize, leftColumnSize, rowId, rightRowId, rightBatch);
                                    rowId++;
                                    break;
                                case SEMI:
                                    if (match) {

                                        assignLeft(leftInput, output, leftColumnSize, rowId);
                                        rowId++;
                                        continue;
                                    }
                                    continue;
                                case ANTI:
                                    if (!match) {

                                        assignLeft(leftInput, output, leftColumnSize, rowId);
                                        rowId++;
                                        continue;
                                    }
                                    break;
                            }
                        }
                    }
                }
                output.setRowCount(rowId);
                return output;
            } finally {
                if (rightBatchIterable != null) {
                    for (VectorSchemaRoot vectorSchemaRoot : rightBatchIterable) {
                        vectorSchemaRoot.close();
                    }
                }
            }
        }).toObservable();
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }

    private Record createRecord(VectorSchemaRoot leftInput, int leftIndex, VectorSchemaRoot rightBatch, int rightIndex) {
        int leftCount = leftInput.getFieldVectors().size();
        IntUnaryOperator function = columnIndex -> {
            if (columnIndex < leftCount) {
                return leftIndex;
            } else {
                return rightIndex;
            }
        };
        return new Record() {
            boolean isNull;

            @Override
            public void setPosition(int value) {

            }

            private int getPosition(int columnIndex) {
                return function.applyAsInt(columnIndex);
            }

            private <T extends FieldVector> T getFieldVector(int columnIndex) {
                FieldVector vector;
                if (columnIndex < leftCount) {
                    vector = leftInput.getVector(columnIndex);
                } else {
                    vector = rightBatch.getVector(columnIndex - leftCount);
                }
                return (T) vector;
            }

            @Override
            public int getInt(int columnIndex) {
                IntVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return vector.get(position);
                }
            }


            @Override
            public long getLong(int columnIndex) {
                BigIntVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return vector.get(position);
                }
            }

            @Override
            public byte getByte(int columnIndex) {
                TinyIntVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return vector.get(position);
                }
            }

            @Override
            public short getShort(int columnIndex) {
                SmallIntVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return vector.get(position);
                }
            }

            @Override
            public BinarySequence getBinary(int columnIndex) {
                VarBinaryVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return null;
                } else {
                    isNull = false;
                    return BinarySequence.of(vector.get(position));
                }
            }

            @Override
            public char getChar(int columnIndex) {
                SmallIntVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return (char) vector.get(position);
                }
            }

            @Override
            public long getDate(int columnIndex) {
                DateDayVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return vector.get(position) * 1000L;
                }
            }

            @Override
            public long getDatetime(int columnIndex) {
                TimeStampVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return vector.get(position) * 1000L;
                }
            }

            @Override
            public CharSequence getString(int columnIndex) {
                VarCharVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return null;
                } else {
                    isNull = false;
                    return new String(vector.get(position));
                }
            }

            @Override
            public CharSequence getSymbol(int columnIndex) {
                return getString(columnIndex);
            }

            @Override
            public float getFloat(int columnIndex) {
                Float4Vector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return vector.get(position);
                }
            }


            @Override
            public double getDouble(int columnIndex) {
                Float8Vector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return vector.get(position);
                }
            }

            @Override
            public long getTime(int columnIndex) {
                TimeStampVector vector = getFieldVector(columnIndex);
                int position = getPosition(columnIndex);
                if (vector.isNull(position)) {
                    isNull = true;
                    return 0;
                } else {
                    isNull = false;
                    return vector.get(position);
                }
            }

            @Override
            public boolean isNull(int columnIndex) {
                return false;
            }

            @Override
            public short getUInt16(int columnIndex) {
                return 0;
            }

            @Override
            public long getUInt64(int columnIndex) {
                return 0;
            }

            @Override
            public Object getObject(int columnIndex) {
                return null;
            }

            @Override
            public int getUInt32(int i) {
                return 0;
            }

            @Override
            public String toString() {
                int columnCount = leftInput.getSchema().getFields().size() + rightBatch.getSchema().getFields().size();
                ArrayList arrayList = new ArrayList();
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    FieldVector vector = getFieldVector(columnIndex);
                    Object object = vector.getObject(function.applyAsInt(columnIndex));
                    arrayList.add(object);
                }
                return arrayList.toString();
            }
        };
    }

    public static void merge(@NotNull VectorSchemaRoot leftInput, VectorSchemaRoot newBatch) {
        int startIndex = leftInput.getRowCount();
        leftInput.setRowCount(startIndex + newBatch.getRowCount());
        for (int i = 0; i < leftInput.getFieldVectors().size(); i++) {
            FieldVector receivesVector = leftInput.getVector(i);
            for (int j = 0; j < newBatch.getRowCount(); j++) {
                receivesVector.copyFrom(j, startIndex+j, newBatch.getVector(i));
            }

        }
    }

    private void assignRight(VectorSchemaRoot output, int rightColumnSize, int leftColumnSize, int rowId, int rightRowId, VectorSchemaRoot rightBatch) {
        for (int rightColumnIndex = 0; rightColumnIndex < rightColumnSize; rightColumnIndex++) {
            output.getVector(leftColumnSize + rightColumnIndex).copyFrom(rightRowId, rowId, rightBatch.getVector(rightColumnIndex));
        }
    }

    private void assignLeft(VectorSchemaRoot leftInput, VectorSchemaRoot output, int leftColumnSize, int rowId) {
        for (int leftColumnIndex = 0; leftColumnIndex < leftColumnSize; leftColumnIndex++) {
            output.getVector(leftColumnIndex).copyFrom(rowId, rowId, leftInput.getVector(leftColumnIndex));
        }
    }

    private void assign(VectorSchemaRoot leftInput, VectorSchemaRoot output, int rightColumnSize, int leftColumnSize, int rowId, int rightRowId, VectorSchemaRoot rightBatch) {
        assignLeft(leftInput, output, leftColumnSize, rowId);
        assignRight(output, rightColumnSize, leftColumnSize, rowId, rightRowId, rightBatch);
    }
}

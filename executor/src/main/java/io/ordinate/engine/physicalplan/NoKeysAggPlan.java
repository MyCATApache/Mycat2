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

import io.ordinate.engine.record.RecordUtil;
import io.ordinate.engine.vector.AggregateVectorExpression;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.function.aggregate.AccumulatorFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class NoKeysAggPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoKeysAggPlan.class);
    Schema schema;
    PhysicalPlan input;
    AccumulatorFunction[] aggregateExprs;

    public NoKeysAggPlan(Schema schema, PhysicalPlan input, AccumulatorFunction[] aggregateExprs) {
        this.schema = schema;
        this.input = input;
        this.aggregateExprs = aggregateExprs;
    }

    public static NoKeysAggPlan create(Schema schema, PhysicalPlan input, AccumulatorFunction[] aggregateExprs){
        return new NoKeysAggPlan(schema,input,aggregateExprs);
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.singletonList(input);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        return input.execute(rootContext).subscribeOn(Schedulers.single())
                .reduce(createAggContext(rootContext),
                        (aggContext, vectorSchemaRoot2) -> aggContext.reduce(vectorSchemaRoot2))
                .map(i -> i.finalToVectorSchemaRoot()).toObservable();
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }

    private AggContext createAggContext(RootContext rootContext) {
        int columnCount = schema().getFields().size();
        int length = aggregateExprs.length;

        AggContext aggContext = new AggContext() {
            SimpleMapValue simpleMapValue;
            AggregateVectorExpression[] aggregateVectorExpressions = new AggregateVectorExpression[aggregateExprs.length];
            @Override
            public void initContext() {
                int columnCount = aggregateExprs.length;
                int longSize = RecordUtil.getContextSize(aggregateExprs);

                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    aggregateVectorExpressions[columnIndex]= aggregateExprs[columnIndex].toAggregateVectorExpression();
                }
                simpleMapValue  = new SimpleMapValue(longSize);
            }

            @Override
            public AggContext reduce(VectorSchemaRoot root) {
                int columnCount = aggregateExprs.length;
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    AggregateVectorExpression aggregateExpr = aggregateVectorExpressions[columnIndex];
                    int inputColumnIndex = aggregateExpr.getInputColumnIndex();
                    FieldVector inputVector = root.getVector(inputColumnIndex);
                    InnerType type = aggregateExpr.getType();

                    switch (type) {
                        case BOOLEAN_TYPE:
                        case INT8_TYPE:
                        case INT16_TYPE:
                        case CHAR_TYPE:
                        case INT32_TYPE:
                        case INT64_TYPE:
                            aggregateExpr.computeUpdateValue(simpleMapValue, inputVector);
                            break;
                        case FLOAT_TYPE:
                            break;
                        case DOUBLE_TYPE:
                            aggregateExpr.computeUpdateValue(simpleMapValue, inputVector);
                            break;
                        case STRING_TYPE:
                            break;
                        case BINARY_TYPE:
                            break;
                        case UINT8_TYPE:
                            break;
                        case UINT16_TYPE:
                            break;
                        case UINT32_TYPE:
                            break;
                        case UINT64_TYPE:
                            break;
                        case TIME_MILLI_TYPE:
                            break;
                        case DATE_TYPE:
                            break;
                        case DATETIME_MILLI_TYPE:
                            break;
                        case SYMBOL_TYPE:
                            break;
                        case OBJECT_TYPE:
                            break;
                        case NULL_TYPE:
                            break;
                    }
                }
                return this;
            }

            @Override
            public VectorSchemaRoot finalToVectorSchemaRoot() {
                Schema schema = schema();
                VectorSchemaRoot output = rootContext.getVectorSchemaRoot(schema, 1);
                output.setRowCount(1);
                int columnCount = aggregateExprs.length;
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    AggregateVectorExpression aggregateExpr = aggregateVectorExpressions[columnIndex];
                    InnerType type = aggregateExpr.getType();
                    switch (type) {
                        case BOOLEAN_TYPE:
                        case INT8_TYPE:
                        case INT16_TYPE:
                        case CHAR_TYPE:
                        case INT32_TYPE:
                        case INT64_TYPE: {
                            ((BigIntVector) output.getVector(columnIndex)).set(0,  aggregateExpr.computeFinalLongValue(simpleMapValue));
                            break;
                        }
                        case FLOAT_TYPE:
                            break;
                        case DOUBLE_TYPE: {
                            ((Float8Vector) output.getVector(columnIndex)).set(0, aggregateExpr.computeFinalDoubleValue(simpleMapValue));
                            break;
                        }
                        case STRING_TYPE:
                            break;
                        case BINARY_TYPE:
                            break;
                        case UINT8_TYPE:
                            break;
                        case UINT16_TYPE:
                            break;
                        case UINT32_TYPE:
                            break;
                        case UINT64_TYPE:
                            break;
                        case TIME_MILLI_TYPE:
                            break;
                        case DATE_TYPE:
                            break;
                        case DATETIME_MILLI_TYPE:
                            break;
                        case SYMBOL_TYPE:
                            break;
                        case OBJECT_TYPE:
                            break;
                        case NULL_TYPE:
                            break;
                    }
                }
                return output;
            }
        };
        aggContext.initContext();
        return aggContext;
    }


    public static interface AggContext {

        public  void initContext();

        public AggContext reduce(VectorSchemaRoot root);

        public VectorSchemaRoot finalToVectorSchemaRoot();
    }
}

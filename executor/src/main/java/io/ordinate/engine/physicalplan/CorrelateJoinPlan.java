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
import io.ordinate.engine.builder.SchemaBuilder;
import io.ordinate.engine.function.bind.VariableParameterFunction;
import io.ordinate.engine.function.bind.VerSetterImpl;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.functions.Function;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.JoinType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.ordinate.engine.physicalplan.NLJoinPlan.merge;

public class CorrelateJoinPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(CorrelateJoinPlan.class);
    PhysicalPlan left;
    JoinType joinType;
    Map<Integer, List<VariableParameterFunction>> variableParameterFunctionMap;
    PhysicalPlan right;
    VerSetterImpl verSetter;

    public CorrelateJoinPlan(PhysicalPlan left,  PhysicalPlan right,JoinType joinType, Map<Integer, List<VariableParameterFunction>> variableParameterFunctionMap) {
        this.left = left;
        this.joinType = joinType;
        this.variableParameterFunctionMap = variableParameterFunctionMap;
        this.right = right;
        this.verSetter = new VerSetterImpl(variableParameterFunctionMap);
    }

    @Override
    public Schema schema() {
        return left.schema();
    }

    @Override
    public List<PhysicalPlan> children() {
        return Arrays.asList(left, right);
    }

    public interface VerSetter {

        public void set(int rowId, VectorSchemaRoot leftInput);
    }

    public class Output {
        private VectorSchemaRoot leftInput;
        private RootContext rootContext;
        VectorSchemaRoot output;
        int outputIndex = 0;

        public Output(VectorSchemaRoot leftInput, RootContext rootContext) {
            this.leftInput = leftInput;
            this.rootContext = rootContext;
            output = rootContext.getVectorSchemaRoot(schema(), leftInput.getRowCount());
        }

        public void addSemi(int leftRowId) {
            if (outputIndex > output.getRowCount()) {
                output.setRowCount(outputIndex);
            }
            SchemaBuilder.copyTo(leftInput, leftRowId, output, outputIndex);
            outputIndex++;
        }

        public void addInner(int leftRowId, VectorSchemaRoot rightInput, int rightRowIndex) {
            if (outputIndex > output.getRowCount()) {
                output.setRowCount(outputIndex);
            }
            SchemaBuilder.joinCopyTo(leftInput, leftRowId, rightInput, rightRowIndex, output, outputIndex);
            outputIndex++;
        }
    }


    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {

        return left.execute(rootContext).map(new Function<VectorSchemaRoot, VectorSchemaRoot>() {
            @Override
            public VectorSchemaRoot apply(VectorSchemaRoot leftInput) throws Throwable {
                int leftRowCount = leftInput.getRowCount();
                Output outputer = new Output(leftInput, rootContext);
                for (int leftRowId = 0; leftRowId < leftRowCount; leftRowId++) {
                    verSetter.set(leftRowId, leftInput);

                    VectorSchemaRoot rightVectorSchemaRoot = right.execute(rootContext).reduce(rootContext.getVectorSchemaRoot(right.schema()), (reduceBatch, newBatch) -> {
                        merge(reduceBatch, newBatch);
                        newBatch.close();
                        return reduceBatch;
                    }).blockingGet();
                    boolean next = rightVectorSchemaRoot.getRowCount() > 0;
                    switch (joinType) {
                        case INNER:
                        case LEFT: {
                            int rightRowCount = rightVectorSchemaRoot.getRowCount();
                            for (int i = 0; i < rightRowCount; i++) {
                                outputer.addInner(leftRowId, rightVectorSchemaRoot, i);
                            }
                            break;
                        }
                        case RIGHT:
                        case FULL:
                        default:
                            throw new UnsupportedOperationException();
                        case SEMI: {
                            if (next) {
                                outputer.addSemi(leftRowId);
                            }
                            break;
                        }
                        case ANTI: {
                            if (!next) {
                                outputer.addSemi(leftRowId);
                            }
                            break;
                        }

                    }
                }
                return outputer.output;
            }
        });
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }

}

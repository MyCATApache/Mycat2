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

import io.ordinate.engine.schema.FieldBuilder;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.vector.VectorContext;
import io.ordinate.engine.vector.VectorExpression;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProjectionPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionPlan.class);
    final PhysicalPlan input;
    final List<VectorExpression> exprs;
    final org.apache.arrow.vector.types.pojo.Schema schema;

    public ProjectionPlan(PhysicalPlan input, List<VectorExpression> exprs, org.apache.arrow.vector.types.pojo.Schema schema) {
        this.input = input;
        this.exprs = exprs;
        this.schema = schema;
    }

    @Override
    public org.apache.arrow.vector.types.pojo.Schema schema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return ImmutableList.of(input);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        FieldVector[] vectorList = new FieldVector[schema().getFields().size()];
        return input.execute(rootContext)
                .subscribeOn(Schedulers.computation())
                .map(input -> {
            try{
                int index = 0;
                for (VectorExpression expr : exprs) {
                    int finalIndex = index;

                    ArrowType type = expr.getType();
                    vectorList[finalIndex]= FieldBuilder.of("", type, expr.isNullable()).toArrow().createVector(rootContext.getRootAllocator());
                    vectorList[finalIndex].setInitialCapacity(input.getRowCount());
                    vectorList[finalIndex].allocateNew();
                    VectorContext vContext1 = new VectorContext() {
                        @Override
                        public VectorSchemaRoot getVectorSchemaRoot() {
                            return input;
                        }

                        @Override
                        public FieldVector getOutputVector() {
                            return    vectorList[finalIndex];
                        }

                        @Override
                        public int getRowCount() {
                            return input.getRowCount();
                        }
                    };
                    expr.eval(vContext1);
                    vectorList[index]=(vContext1.getOutputVector());
                    vContext1.free();
                    index++;
                }
                VectorSchemaRoot res = VectorSchemaRoot.of(vectorList);
                res.setRowCount(input.getRowCount());
                return res;
            } finally {
                ProjectionPlan.this.input.eachFree(input);
            }
        });
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }

    @Override
    public String toString() {
        return "Projection:" + exprs;
    }
}

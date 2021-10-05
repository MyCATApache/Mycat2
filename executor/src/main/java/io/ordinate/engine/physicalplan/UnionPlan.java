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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class UnionPlan implements PhysicalPlan{
    private static final Logger LOGGER = LoggerFactory.getLogger(UnionPlan.class);
    PhysicalPlan[] physicalPlans;

    public UnionPlan(PhysicalPlan[] physicalPlans) {
        this.physicalPlans = physicalPlans;
    }

    public static UnionPlan create( PhysicalPlan[] physicalPlans){
        return new UnionPlan(physicalPlans);
    }

    @Override
    public Schema schema() {
        return physicalPlans[0].schema();
    }

    @Override
    public List<PhysicalPlan> children() {
        return Arrays.asList(physicalPlans);
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        return Observable.fromArray(physicalPlans).flatMap(i->i.execute(
                rootContext
        ));
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }
}

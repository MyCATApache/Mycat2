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
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.bind.IntBindVariable;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LimitPlan implements PhysicalPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(LimitPlan.class);
    final PhysicalPlan input;
    private IntFunction offsetExpr;
    private IntFunction fetchExpr;

    public static LimitPlan create(PhysicalPlan input, IntFunction offset, IntFunction fetch){
        return new LimitPlan(input,offset,fetch);
    }

    public LimitPlan(PhysicalPlan input, IntFunction offset, IntFunction fetch) {
        this.input = input;
        this.offsetExpr = offset;
        this.fetchExpr = fetch;
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
        MutableLong offsetBox = new MutableLong(offsetExpr.getInt(null));
        MutableLong fetchBox= new MutableLong(fetchExpr.getInt(null));
        Observable<VectorSchemaRoot> observable = input.execute(rootContext).subscribeOn(Schedulers.single());
        if (offsetBox.longValue() > 0) {
            observable = observable.map(root -> {
                long offset = offsetBox.longValue();
                if (offset == 0) {
                    return root;
                }
                if (offset > 0) {
                    if (root.getRowCount() > offset) {
                        offsetBox.setValue(0);
                        return root.slice((int) offset);
                    }
                    offsetBox.subtract(root.getRowCount());
                    root.clear();
                    return root;
                }
                throw new IllegalArgumentException();
            });
        }
        if (fetchBox.longValue() > 0) {
            observable = observable.map(root -> {
                long fetch = fetchBox.longValue();
                if (fetch == 0) {
                    root.clear();
                    return root;
                }
                int rowCount = root.getRowCount();
                if (fetch > 0) {
                    if (fetch > rowCount) {
                        fetchBox.subtract(rowCount);
                        return root;
                    }

                    fetchBox.setValue(0);
                    return root.slice(0, (int) fetch);
                }
                throw new IllegalArgumentException();
            });
        }
        return observable;
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        physicalPlanVisitor.visit(this);
    }
}

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

import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.schema.IntInnerType;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.sources.In;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.IntStream;

import static io.ordinate.engine.schema.InnerType.fromSchemaToIntInnerTypes;

public interface PhysicalPlan {

    org.apache.arrow.vector.types.pojo.Schema schema();

    List<PhysicalPlan> children();

    @NotNull
    default IntInnerType[] getIntTypes() {
       return fromSchemaToIntInnerTypes(schema());
    }
    default String toPrettyString() {
        return format(this);
    }

    default public String format(PhysicalPlan physicalPlan) {
        return format(physicalPlan, 0);
    }

    default public String format(PhysicalPlan physicalPlan, int level) {
        StringBuilder b = new StringBuilder();
        IntStream.range(0, level).forEach(unused -> b.append("\t"));
        b.append(physicalPlan.toString()).append("\n");
        physicalPlan.children().forEach(it -> b.append(format(it, level + 1)));
        return b.toString();
    }

    Observable<VectorSchemaRoot> execute(RootContext rootContext);

    default void eachFree(VectorSchemaRoot vectorSchemaRoot){

    }

   default void close(){

   }

   void accept(PhysicalPlanVisitor physicalPlanVisitor);
}

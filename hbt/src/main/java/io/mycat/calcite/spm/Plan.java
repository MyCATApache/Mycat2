/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.spm;

import io.mycat.DrdsSql;
import io.mycat.MycatDataContext;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;

public interface Plan extends Comparable<Plan> {

    boolean forUpdate();

    RelOptCost getRelOptCost();

    public Type getType();

    public CodeExecuterContext getCodeExecuterContext();

    public RelNode getPhysical();

    default List<String> explain(MycatDataContext dataContext, DrdsSql drdsSql) {
        return explain(dataContext, drdsSql, true);
    }

    List<String> explain(MycatDataContext dataContext, DrdsSql drdsSql, boolean code);

    static public enum Type {
        PHYSICAL,
        UPDATE,
        INSERT
    }

    public default CalciteRowMetaData getMetaData() {
        return new CalciteRowMetaData(getPhysical().getRowType().getFieldList());
    }

    @NotNull
    public   String dumpPlan();
    @NotNull
     List<SpecificSql> specificSql(DrdsSql drdsSql);



}
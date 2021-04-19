/**
 * Copyright (C) <2021>  <chen junwen>
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
import io.mycat.DrdsSqlWithParams;
import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;

public interface Plan  {

    boolean forUpdate();

    public Type getType();

    public CodeExecuterContext getCodeExecuterContext();

    public MycatUpdateRel getUpdatePhysical() ;

    public MycatInsertRel getInsertPhysical() ;

    public MycatRel getMycatRel() ;

    default List<String> explain(MycatDataContext dataContext, DrdsSqlWithParams drdsSql) {
        return explain(dataContext, drdsSql, true);
    }

    List<String> explain(MycatDataContext dataContext, DrdsSqlWithParams drdsSql, boolean code);

    static public enum Type {
        PHYSICAL,
        UPDATE,
        INSERT
    }

    public default MycatRowMetaData getMetaData() {
      return null;
    }

    @NotNull
    public   String dumpPlan();
    @NotNull
     List<SpecificSql> specificSql(DrdsSqlWithParams drdsSql);



}
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
package io.mycat.calcite.physical;

import io.mycat.DrdsSqlCompiler;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatRel;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

@Getter
public class MycatInsertRel extends AbstractRelNode implements MycatRel {

    private final static RelOptCluster cluster = DrdsSqlCompiler.newCluster();
    private final static RelDataType rowType = RelOptUtil.createDmlRowType(
            SqlKind.INSERT, cluster.getTypeFactory());
    private final MycatRouteInsertCore mycatRouteInsertCore;


    public static MycatInsertRel create(
            int finalAutoIncrementIndex, List<Integer> shardingKeys,String sql, String schemaName, String tableName) {
        return new MycatInsertRel(new MycatRouteInsertCore(finalAutoIncrementIndex,shardingKeys, sql,schemaName,tableName));
    }

    public static MycatInsertRel create(
            MycatRouteInsertCore mycatRouteInsertCore) {
        return new MycatInsertRel(mycatRouteInsertCore);
    }

    public MycatInsertRel(
            MycatRouteInsertCore mycatRouteInsertCore) {
        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
        this.mycatRouteInsertCore = mycatRouteInsertCore;
    }



    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatInsertRel").into();
        return writer.ret();
    }

}
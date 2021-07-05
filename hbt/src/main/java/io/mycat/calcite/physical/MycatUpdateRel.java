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

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.DrdsSqlCompiler;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.rewriter.IndexCondition;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

@Getter
public class MycatUpdateRel extends AbstractRelNode implements MycatRel {
    private final MycatRouteUpdateCore mycatRouteUpdateCore;
    private static RelOptCluster cluster = DrdsSqlCompiler.newCluster();
    private static final RelDataType dmlRowType = RelOptUtil.createDmlRowType(SqlKind.INSERT, cluster.getTypeFactory());


    public MycatUpdateRel(SQLStatement sqlStatement, String schemaName, String tableName) {
        this(new MycatRouteUpdateCore(sqlStatement, schemaName, tableName, false, null));
    }

    public MycatUpdateRel(SQLStatement sqlStatement, String schemaName, String tableName, RexNode condition) {
        this(new MycatRouteUpdateCore(sqlStatement, schemaName, tableName, false, condition));
    }

    public MycatUpdateRel(SQLStatement sqlStatement, String schemaName, String tableName, boolean global, RexNode condition) {
        this(new MycatRouteUpdateCore(sqlStatement, schemaName, tableName, global, condition));
    }

    public MycatUpdateRel(MycatRouteUpdateCore mycatRouteUpdateCore) {
        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
        this.mycatRouteUpdateCore = mycatRouteUpdateCore;
        this.rowType = dmlRowType;
    }

    public static MycatUpdateRel create(SQLStatement sqlStatement, String schemaName, String tableName,RexNode condition) {
        return new MycatUpdateRel(new MycatRouteUpdateCore(sqlStatement, schemaName, tableName, false, condition));
    }
    public static MycatUpdateRel create(SQLStatement sqlStatement, String schemaName, String tableName, boolean global, RexNode conditions) {
        return new MycatUpdateRel(new MycatRouteUpdateCore(sqlStatement, schemaName, tableName, global, conditions));
    }

    public static MycatUpdateRel create(MycatRouteUpdateCore mycatRouteUpdateCore) {
        return new MycatUpdateRel(mycatRouteUpdateCore);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return mycatRouteUpdateCore.explain(writer);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return mycatRouteUpdateCore.explainTerms(pw);
    }

    public boolean isGlobal() {
        return mycatRouteUpdateCore.isGlobal();
    }

    public SQLStatement getSqlStatement() {
        return mycatRouteUpdateCore.getSqlStatement();
    }

    public String getSchemaName() {
        return mycatRouteUpdateCore.getSchemaName();
    }

    public String getTableName() {
        return mycatRouteUpdateCore.getTableName();
    }

    public RexNode getConditions() {
        return mycatRouteUpdateCore.getConditions();
    }
}
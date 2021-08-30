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
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
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
    private static RelOptCluster cluster = DrdsSqlCompiler.newCluster();
    private static final RelDataType dmlRowType = RelOptUtil.createDmlRowType(SqlKind.INSERT, cluster.getTypeFactory());
    private final SQLStatement sqlStatement;
    private boolean global;
    public MycatUpdateRel(SQLStatement sqlStatement) {
       this(sqlStatement,false);
    }

    public MycatUpdateRel(SQLStatement sqlStatement,boolean global) {
        super(cluster,cluster.traitSet());
        this.sqlStatement = sqlStatement;
        this.global = global;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return null;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.item("sql",sqlStatement.toString());
        return pw;
    }

    public SQLStatement getSqlStatement() {
        return sqlStatement;
    }

    public boolean isInsert(){
        return sqlStatement instanceof SQLInsertStatement;
    }
}
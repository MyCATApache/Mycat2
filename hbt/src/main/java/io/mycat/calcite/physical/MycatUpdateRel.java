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
import io.mycat.DataNode;
import io.mycat.calcite.*;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.DrdsRunner;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class MycatUpdateRel extends AbstractRelNode implements MycatRel {
   final Distribution values;
    final SQLStatement sqlStatement;
    final  boolean global;
    final  List<RexNode> conditions;
    private static RelOptCluster cluster = DrdsRunner.newCluster();

    public static MycatUpdateRel create(Distribution values, SQLStatement sqlStatement,boolean global) {
        return new MycatUpdateRel(values, sqlStatement,global);
    }

    public static MycatUpdateRel create(RelOptCluster cluster, Distribution values, SQLStatement sqlStatement,boolean global) {
        return new MycatUpdateRel(cluster, values, sqlStatement,Collections.emptyList(),global);
    }

    public MycatUpdateRel(Distribution values, SQLStatement sqlStatement,boolean global) {
        this(cluster, values, sqlStatement,Collections.emptyList(),global);
    }

    public MycatUpdateRel(RelOptCluster cluster, Distribution values, SQLStatement sqlStatement,List<RexNode> conditions,boolean global) {
        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
        this.values = values;
        this.sqlStatement = sqlStatement;
        this.global = global;
        this.rowType = RelOptUtil.createDmlRowType(SqlKind.INSERT, getCluster().getTypeFactory());
        this.conditions = conditions;
    }

    public static MycatUpdateRel create(RelOptCluster cluster, Distribution distribution, List<RexNode> conditions, SQLStatement sqlStatement) {
        return new MycatUpdateRel(cluster,distribution,sqlStatement,conditions,false);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatUpdateRel").into();
        writer.item("sql",sqlStatement);
        writer.item("dataNodes",values);
        return writer.ret();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.item("sql",sqlStatement+"\n");
        int index = 0;
        for (DataNode dataNode : values.getDataNodes().flatMap(i->i.values().stream()).collect(Collectors.toList())) {
            pw.item("dataNodes$"+index,dataNode+"\n");
        }
        return pw;
    }
}
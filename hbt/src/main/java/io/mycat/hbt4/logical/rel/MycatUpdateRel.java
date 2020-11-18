package io.mycat.hbt4.logical.rel;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.DataNode;
import io.mycat.hbt3.Distribution;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt4.*;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlKind;

@Getter
public class MycatUpdateRel extends AbstractRelNode implements MycatRel {
    Distribution values;
    SQLStatement sqlStatement;
    private static RelOptCluster cluster = DrdsRunner.newCluster();

    public static MycatUpdateRel create(Distribution values, SQLStatement sqlStatement) {
        return new MycatUpdateRel(values, sqlStatement);
    }

    public static MycatUpdateRel create(RelOptCluster cluster, Distribution values, SQLStatement sqlStatement) {
        return new MycatUpdateRel(cluster, values, sqlStatement);
    }

    public MycatUpdateRel(Distribution values, SQLStatement sqlStatement) {
        this(cluster, values, sqlStatement);
    }

    public MycatUpdateRel(RelOptCluster cluster, Distribution values, SQLStatement sqlStatement) {
        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
        this.values = values;
        this.sqlStatement = sqlStatement;
        this.rowType = RelOptUtil.createDmlRowType(
                SqlKind.INSERT, getCluster().getTypeFactory());
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatUpdateRel").into();
        writer.item("sql",sqlStatement);
        writer.item("dataNodes",values);
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.item("sql",sqlStatement+"\n");
        int index = 0;
        for (DataNode dataNode : values.getDataNodes()) {
            pw.item("dataNodes$"+index,dataNode+"\n");
        }
        return pw;
    }
}
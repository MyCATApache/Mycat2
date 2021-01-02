package io.mycat.calcite.table;

import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.Executor;
import io.mycat.calcite.ExecutorImplementor;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class MycatTransientSQLTableScan extends AbstractRelNode implements MycatRel {
    final String sql;
    final String targetName;

    public MycatTransientSQLTableScan(RelOptCluster cluster, RelDataType relDataType, String targetName, String sql) {
        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
        this.targetName = targetName;
        this.sql = sql;
        this.rowType = relDataType;
    }


    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatTransientSQLTableScan(getCluster(), this.rowType, targetName, sql);
    }

    public String getSql() {
        return sql;
    }

    public String getTargetName() {
        return targetName;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("target",targetName).item("sql",sql);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatTransientSQLTableScan").into()
                .item("target", targetName)
                .item("sql", sql)
                .ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        return null;
    }
}
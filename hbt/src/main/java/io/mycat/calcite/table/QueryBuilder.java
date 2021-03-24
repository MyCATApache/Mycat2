package io.mycat.calcite.table;

import io.mycat.calcite.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.Optional;
import java.util.stream.StreamSupport;

public abstract class QueryBuilder extends AbstractRelNode implements MycatRel {

    public void setRowType(RelDataType rowType){
        this.rowType = rowType;
    }
    public QueryBuilder(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet);
    }

    public QueryBuilder(RelOptCluster cluster) {
        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
    }

    public abstract Optional<QueryBuilder> filter(RexNode condition);

    public abstract Optional<QueryBuilder> project(int[] copyOf);

    public abstract Optional<QueryBuilder> sort(Long offsetNumber,
                                                Long fetchNumber,
                                                RelCollation collation);

    public static QueryBuilder createDefaultQueryBuilder(RelOptCluster cluster,
                                                         String name,
                                                         Iterable<Object[]> rows){
        return createRowDefaultQueryBuilder(cluster,name, ()->StreamSupport.stream(
                rows.spliterator(),false).iterator());
    }

    public static QueryBuilder createRowDefaultQueryBuilder(RelOptCluster cluster,
                                                         String name,
                                                         Iterable<Object[]> rows) {
        return new QueryBuilder(cluster) {
            @Override
            public Optional<QueryBuilder> filter(RexNode condition) {
                return Optional.empty();
            }

            @Override
            public Optional<QueryBuilder> project(int[] copyOf) {
                return Optional.empty();
            }

            @Override
            public Optional<QueryBuilder> sort(Long offsetNumber, Long fetchNumber, RelCollation collation) {
                return Optional.empty();
            }


            @Override
            public ExplainWriter explain(ExplainWriter writer) {
                return writer.name(name).into().ret();
            }
        };
    }

}
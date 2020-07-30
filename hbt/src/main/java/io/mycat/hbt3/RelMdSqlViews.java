package io.mycat.hbt3;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;

import java.util.Objects;

public class RelMdSqlViews {

    final static NextConvertor nextConvertor = new NextConvertor();

    static {
        nextConvertor.put(TableScan.class,
                Project.class, Union.class, Aggregate.class, Sort.class, Filter.class, Join.class);
        nextConvertor.put(Filter.class,
                Project.class, Union.class, Aggregate.class, Sort.class);
        nextConvertor.put(Join.class,
                Project.class, Union.class, Aggregate.class, Filter.class, Join.class, Sort.class);
        nextConvertor.put(Project.class,
                Project.class, Union.class, Aggregate.class, Sort.class);
        nextConvertor.put(Aggregate.class,
                Project.class, Union.class, Filter.class, Sort.class);
    }

    public static boolean onTableScan(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(TableScan.class, relNode.getClass());
    }
    public static boolean onFilter(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(Filter.class, relNode.getClass());
    }
    public static boolean onJoin(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(Join.class, relNode.getClass());
    }
    public static boolean onProject(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(Project.class, relNode.getClass());
    }
    public static boolean onAggregate(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(Aggregate.class, relNode.getClass());
    }
    public static boolean onRelNode(RelNode base,RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(base.getClass(), relNode.getClass());
    }
}
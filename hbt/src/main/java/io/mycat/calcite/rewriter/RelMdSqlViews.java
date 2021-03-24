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

package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;

import java.util.List;
import java.util.Objects;

public class RelMdSqlViews {

    final static NextConvertor nextConvertor = new NextConvertor();
    final static List<RelOptRule> RULES;

    static {
        nextConvertor.put(TableScan.class,
                Project.class, Union.class, Aggregate.class, Sort.class, Filter.class, Join.class);
        nextConvertor.put(Filter.class,
                Project.class, Union.class, Aggregate.class, Sort.class,Filter.class,Join.class);
        nextConvertor.put(Join.class,
                Project.class, Union.class, Aggregate.class, Filter.class, Join.class, Sort.class);
        nextConvertor.put(Project.class,
                Project.class, Union.class, Aggregate.class, Sort.class,Filter.class);
        nextConvertor.put(Aggregate.class,
                Project.class, Union.class, Filter.class, Sort.class);

        ImmutableList.Builder<RelOptRule> builder = ImmutableList.builder();
        nextConvertor.map.forEach((base, ups) -> {
            for (Class up : ups) {
                nextConvertor.check(base,up);
//                builder.add(new RBORule(base,up));
            }
        });
        RULES = builder.build();
    }

    public static boolean onTableScan(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(TableScan.class, relNode.getClass());
    }

    public static boolean onFilter(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(Filter.class, relNode.getClass());
    }

    public static boolean filter(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(relNode, Filter.class);
    }

    public static boolean onJoin(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(Join.class, relNode.getClass());
    }

    public static boolean join(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(relNode, Join.class);
    }

    public static boolean onProject(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(Project.class, relNode.getClass());
    }

    public static boolean project(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(relNode, Project.class);
    }

    public static boolean onAggregate(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(Aggregate.class, relNode.getClass());
    }

    public static boolean aggregate(RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(relNode, Aggregate.class);
    }

    public static boolean onRelNode(RelNode base, RelNode relNode) {
        Objects.requireNonNull(relNode);
        return nextConvertor.check(base.getClass(), relNode.getClass());
    }

    public static boolean correlate(RelNode left) {
        return false;
    }

    public static boolean sort(RelNode input) {
        return true;
    }
}
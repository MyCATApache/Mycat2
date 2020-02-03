package io.mycat.calcite.logic;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface MycatRel extends RelNode {
    void implement(Implementor implementor);

    final Convention CONVENTION = new Convention.Impl("MYCAT", MycatRel.class);

    /**
     * Callback for the implementation process that converts a tree of
     * {@link MycatRel} nodes into a MycatSQL query.
     */
    class Implementor {
        final Map<String, String> selectFields = new LinkedHashMap<>();
        final List<String> whereClause = new ArrayList<>();
        int offset = 0;
        int fetch = -1;
        final List<String> order = new ArrayList<>();
        RelOptTable  relOptTable;
        MycatTableBase tableBase;

        /**
         * Adds newly projected fields and restricted predicates.
         *
         * @param fields     New fields to be projected from a query
         * @param predicates New predicates to be applied to the query
         */
        public void add(Map<String, String> fields, List<String> predicates) {
            if (fields != null) {
                selectFields.putAll(fields);
            }
            if (predicates != null) {
                whereClause.addAll(predicates);
            }
        }

        public void addOrder(List<String> newOrder) {
            order.addAll(newOrder);
        }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((MycatRel) input).implement(this);
        }
    }
}
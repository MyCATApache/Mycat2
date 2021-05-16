package io.mycat.calcite.logical;

import io.mycat.calcite.table.MycatLogicTable;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalSort;

import java.util.HashMap;
import java.util.Map;

@Getter
public class ViewInfo extends RelShuttleImpl {
        boolean containsOrder = false;
        Map<String, MycatLogicTable> aliasTableMap = new HashMap<>();
        @Override
        public RelNode visit(LogicalSort sort) {
            containsOrder = true;
            return sort;
        }

        @Override
        public RelNode visit(TableScan scan) {
            MycatLogicTable mycatLogicTable = scan.getTable().unwrap(MycatLogicTable.class);
            for (RelHint hint : scan.getHints()) {
               if("QN_NAME".equalsIgnoreCase( hint.hintName)){
                   aliasTableMap.put(hint.listOptions.get(0),mycatLogicTable);
               }
            }

            return super.visit(scan);
        }
    }

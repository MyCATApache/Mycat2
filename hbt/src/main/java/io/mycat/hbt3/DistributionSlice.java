package io.mycat.hbt3;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

public class DistributionSlice {
    public RelNode rewrite(TableScan scan) {
        return null;
    }
}
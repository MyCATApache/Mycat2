package io.mycat.calcite;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

public class MycatRelBuilder extends RelBuilder {
    protected MycatRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    public static void main(String[] args) {

    }
}
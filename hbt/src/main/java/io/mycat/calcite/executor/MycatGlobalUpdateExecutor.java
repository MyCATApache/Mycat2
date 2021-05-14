package io.mycat.calcite.executor;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.MycatDataContext;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.rewriter.Distribution;

import java.util.List;

public class MycatGlobalUpdateExecutor extends MycatUpdateExecutor {
    public MycatGlobalUpdateExecutor(MycatDataContext context, MycatUpdateRel mycatUpdateRel, SQLStatement sqlStatement, List<Object> parameters) {
        super(context, mycatUpdateRel, sqlStatement, parameters);
    }

    @Override
    public long getAffectedRow() {
        return super.getAffectedRow() / this.getReallySqlSet().size();
    }

    @Override
    public boolean isProxy() {
        return false;
    }
}

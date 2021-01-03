package io.mycat.calcite.executor;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.MycatDataContext;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.DataSourceFactory;

import java.util.List;

public class MycatGlobalUpdateExecutor extends MycatUpdateExecutor {
    public MycatGlobalUpdateExecutor(MycatDataContext context, Distribution values, SQLStatement sqlStatement, List<Object> parameters, DataSourceFactory factory) {
        super(context, values, sqlStatement, parameters, factory);
    }

    @Override
    public long getAffectedRow() {
        return super.getAffectedRow() / this.getReallySqlSet().size();
    }
}
